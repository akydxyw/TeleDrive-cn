package com.drdisagree.teledrive.presentation

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.drdisagree.teledrive.core.security.AppLockManager
import com.drdisagree.teledrive.core.telegram.TelegramAuthState
import com.drdisagree.teledrive.domain.model.BackupTrigger
import com.drdisagree.teledrive.domain.model.FolderChange
import com.drdisagree.teledrive.domain.model.LayoutDensity
import com.drdisagree.teledrive.domain.repository.BackupRepository
import com.drdisagree.teledrive.domain.repository.ChannelRepository
import com.drdisagree.teledrive.domain.repository.SettingsRepository
import com.drdisagree.teledrive.domain.repository.SyncRepository
import com.drdisagree.teledrive.domain.repository.TelegramAuthRepository
import com.drdisagree.teledrive.presentation.common.UiText
import com.drdisagree.teledrive.presentation.common.toUiText
import com.drdisagree.teledrive.resources.Res
import com.drdisagree.teledrive.resources.error_unknown
import com.drdisagree.teledrive.resources.home_backing_up_new_files
import com.drdisagree.teledrive.resources.home_nothing_new_to_back_up
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import com.drdisagree.teledrive.core.update.AppRelease
import com.drdisagree.teledrive.core.update.UpdateChecker
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import com.drdisagree.teledrive.core.common.AppResult
import com.drdisagree.teledrive.data.repository.FolderChangeNotifier
import com.drdisagree.teledrive.data.repository.LocalDataWiper
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

class AppViewModel(
    private val settingsRepository: SettingsRepository,
    private val telegramAuthRepository: TelegramAuthRepository,
    private val syncRepository: SyncRepository,
    private val backupRepository: BackupRepository,
    private val channelRepository: ChannelRepository,
    private val appLockManager: AppLockManager,
    private val localDataWiper: LocalDataWiper,
    private val updateChecker: UpdateChecker,
    private val folderChangeNotifier: FolderChangeNotifier
) : ViewModel() {

    private val _sessionBroken = MutableStateFlow(false)
    val sessionBroken: StateFlow<Boolean> = _sessionBroken.asStateFlow()

    private val _driveMissing = MutableSharedFlow<Unit>(extraBufferCapacity = 1)
    val driveMissing = _driveMissing.asSharedFlow()

    private val _pendingUpdate = MutableStateFlow<AppRelease?>(null)
    val pendingUpdate: StateFlow<AppRelease?> = _pendingUpdate.asStateFlow()

    private val _messages = MutableSharedFlow<UiText>(extraBufferCapacity = 4)
    val messages: SharedFlow<UiText> = _messages.asSharedFlow()

    val pendingFolderChanges: StateFlow<List<FolderChange>> = folderChangeNotifier.pending

    @Volatile
    private var driveReady = false
    private var following: Job? = null

    val uiState: StateFlow<AppUiState> = combine(
        settingsRepository.preferences,
        appLockManager.locked
    ) { prefs, locked ->
        AppUiState(
            loading = false,
            onboardingComplete = prefs.onboardingComplete,
            theme = prefs.theme,
            language = prefs.language,
            dynamicColor = prefs.dynamicColor,
            compactLayout = prefs.layoutDensity == LayoutDensity.COMPACT,
            locked = locked
        )
    }.stateIn(viewModelScope, SharingStarted.Eagerly, AppUiState())

    init {
        viewModelScope.launch {
            appLockManager.onAppStarted()
            backupRepository.refreshActiveSession()
            val started = telegramAuthRepository.startFromStoredCredentials()
            if (started is AppResult.Failure) _sessionBroken.value = true
        }
        checkForUpdate()
        combine(
            telegramAuthRepository.authState.map { it == TelegramAuthState.Ready },
            uiState.map { it.onboardingComplete }
        ) { ready, onboarded -> ready && onboarded }
            .distinctUntilChanged()
            .onEach { ready ->
                if (!ready) {
                    driveReady = false
                    stopFollowing()
                }
            }
            .filter { it }
            .onEach {
                channelRepository.refreshKnown()
                if (channelRepository.activeDriveMissing()) {
                    _driveMissing.tryEmit(Unit)
                    return@onEach
                }
                driveReady = true
                syncRepository.syncOnStart()
                checkFolderChangesOnStart()
                viewModelScope.launch { syncRepository.catchUpWithRemote() }
                startFollowing()
            }
            .launchIn(viewModelScope)
    }

    /**
     * With the option enabled, the start-up backup waits for the user: only the folders they
     * tick in the dialog are synced. Otherwise the usual silent catch-up runs.
     */
    private suspend fun checkFolderChangesOnStart() {
        val enabled = settingsRepository.preferences.first().folderChangeSyncEnabled
        if (!enabled) {
            catchUpBackup()
            return
        }
        val changes = runCatching { backupRepository.scanFolderChanges() }.getOrNull()
        if (changes is AppResult.Success && changes.value.isNotEmpty()) {
            folderChangeNotifier.publish(changes.value)
        } else {
            catchUpBackup()
        }
    }

    fun syncFolderChanges(folderPaths: Set<String>) {
        if (folderPaths.isEmpty()) return
        folderChangeNotifier.clear()
        viewModelScope.launch {
            val result = runCatching { backupRepository.applyFolderChanges(folderPaths) }.getOrNull()
            _messages.tryEmit(
                when {
                    result == null -> UiText.Resource(Res.string.error_unknown)
                    result is AppResult.Failure -> result.error.toUiText()
                    result is AppResult.Success && result.value == null ->
                        UiText.Resource(Res.string.home_nothing_new_to_back_up)

                    else -> UiText.Resource(Res.string.home_backing_up_new_files)
                }
            )
        }
    }

    fun dismissFolderChanges() = folderChangeNotifier.clear()

    /** Folders live with the active drive, so the check has to ask the channel. */
    private suspend fun catchUpBackup() {
        val chatId = settingsRepository.preferences.first().storageChatId ?: return
        if (channelRepository.backupFolders(chatId).isEmpty()) return
        backupRepository.startBackup(BackupTrigger.SCHEDULED)
    }

    fun onAppStopped() {
        appLockManager.onAppStopped()
        stopFollowing()
    }

    fun onAppStarted() {
        viewModelScope.launch { appLockManager.onAppStarted() }
        if (driveReady) {
            viewModelScope.launch { syncRepository.catchUpWithRemote() }
            startFollowing()
        }
    }

    private fun startFollowing() {
        if (following?.isActive == true) return
        following = viewModelScope.launch { syncRepository.followRemoteChanges() }
    }

    private fun stopFollowing() {
        following?.cancel()
        following = null
    }

    /**
     * Runs at most once a day whatever the notification setting says;
     * [force] is for the notification tap, which asks for the answer now.
     */
    fun checkForUpdate(force: Boolean = false) {
        viewModelScope.launch {
            val prefs = settingsRepository.preferences.first()
            if (!prefs.onboardingComplete) return@launch
            val elapsed = System.currentTimeMillis() - prefs.lastUpdateCheckAt
            if (!force && elapsed < UPDATE_CHECK_INTERVAL_MS) return@launch

            settingsRepository.update { it.copy(lastUpdateCheckAt = System.currentTimeMillis()) }
            val release = updateChecker.newerRelease()
            if (!force && release?.version == prefs.skippedUpdateVersion) return@launch
            _pendingUpdate.value = release
        }
    }

    fun resetSession() {
        viewModelScope.launch {
            withContext(NonCancellable) {
                telegramAuthRepository.logout()
                localDataWiper.wipe()
                settingsRepository.clearTelegramCredentials()
                settingsRepository.update {
                    it.copy(
                        onboardingComplete = false,
                        storageChatId = null,
                        backupFolders = emptySet()
                    )
                }
            }
            _sessionBroken.value = false
        }
    }

    fun dismissUpdate() {
        _pendingUpdate.value = null
    }

    fun unlock() = appLockManager.unlock()

    private companion object {
        const val UPDATE_CHECK_INTERVAL_MS = 24L * 60 * 60 * 1000
    }
}
