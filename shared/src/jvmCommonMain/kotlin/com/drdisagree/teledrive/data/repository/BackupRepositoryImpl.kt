package com.drdisagree.teledrive.data.repository

import com.drdisagree.teledrive.core.common.AppError
import com.drdisagree.teledrive.core.common.AppResult
import com.drdisagree.teledrive.core.common.SafeLog
import com.drdisagree.teledrive.core.dispatchers.DispatcherProvider
import com.drdisagree.teledrive.core.files.AppStoragePaths
import com.drdisagree.teledrive.core.files.Hashing
import com.drdisagree.teledrive.core.files.MimeTypes
import com.drdisagree.teledrive.core.media.MediaMetadataExtractor
import com.drdisagree.teledrive.core.transfer.BackupSessionTracker
import com.drdisagree.teledrive.data.local.dao.BackupDao
import com.drdisagree.teledrive.data.local.dao.FileDao
import com.drdisagree.teledrive.data.local.dao.TransferDao
import com.drdisagree.teledrive.data.local.entity.BackupRecordEntity
import com.drdisagree.teledrive.data.local.entity.BackupSessionEntity
import com.drdisagree.teledrive.data.local.entity.FileEntity
import com.drdisagree.teledrive.data.local.entity.TransferEntity
import com.drdisagree.teledrive.data.mapper.toDomain
import com.drdisagree.teledrive.domain.model.BackupDecision
import com.drdisagree.teledrive.domain.model.BackupSession
import com.drdisagree.teledrive.domain.model.BackupSessionStatus
import com.drdisagree.teledrive.domain.model.BackupState
import com.drdisagree.teledrive.domain.model.BackupTrigger
import com.drdisagree.teledrive.domain.model.ExclusionType
import com.drdisagree.teledrive.domain.model.FileCategory
import com.drdisagree.teledrive.domain.model.FolderChange
import com.drdisagree.teledrive.domain.model.TransferState
import com.drdisagree.teledrive.domain.repository.BackupRepository
import com.drdisagree.teledrive.domain.repository.ChannelRepository
import com.drdisagree.teledrive.domain.repository.ExclusionRepository
import com.drdisagree.teledrive.domain.repository.FileRepository
import com.drdisagree.teledrive.domain.repository.SettingsRepository
import com.drdisagree.teledrive.domain.repository.TrashRepository
import com.drdisagree.teledrive.domain.usecase.DecideBackupActionUseCase
import com.drdisagree.teledrive.domain.usecase.ExclusionCandidate
import com.drdisagree.teledrive.domain.usecase.ExistingBackupRecord
import java.io.File
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext

class BackupRepositoryImpl(
    private val backupDao: BackupDao,
    private val fileDao: FileDao,
    private val transferDao: TransferDao,
    private val transferRepository: TransferRepositoryImpl,
    private val backupSessionTracker: BackupSessionTracker,
    private val exclusionRepository: ExclusionRepository,
    private val settingsRepository: SettingsRepository,
    private val decideBackupAction: DecideBackupActionUseCase,
    private val mediaMetadataExtractor: MediaMetadataExtractor,
    private val folderPathResolver: FolderPathResolver,
    private val fileRepository: FileRepository,
    private val channelRepository: ChannelRepository,
    private val trashRepository: TrashRepository,
    private val storagePaths: AppStoragePaths,
    private val dispatchers: DispatcherProvider
) : BackupRepository {

    override fun observeActiveSession(): Flow<BackupSession?> =
        backupDao.observeActiveSession().map { it?.toDomain() }

    override fun observeLastBackupAt(): Flow<Long?> = backupDao.observeLastBackupAt()

    override suspend fun startBackup(trigger: BackupTrigger): AppResult<String?> =
        withContext(dispatchers.io) { runBackup(trigger) }

    private suspend fun runBackup(
        trigger: BackupTrigger,
        folderFilter: Set<String>? = null
    ): AppResult<String?> {
        if (backupDao.activeSession() != null) {
            return AppResult.Failure(AppError.BackupAlreadyRunning)
        }

        val prefs = settingsRepository.preferences.first()
        val activeChatId = prefs.storageChatId
        val folders = if (activeChatId != null) {
            channelRepository.backupFolders(activeChatId)
        } else {
            emptySet()
        }.let { all ->
            if (folderFilter == null) all else all.filter { it in folderFilter }.toSet()
        }
        if (folders.isEmpty()) {
            return AppResult.Failure(
                AppError.UnsupportedOperation(
                    "No backup folders selected. Pick folders in Settings, " +
                            "or upload files from Files."
                )
            )
        }

        backupDao.deleteOrphanedRecords()
        val exclusions = exclusionRepository.getEnabled()
        val maxSizeBytes = prefs.backupMaxFileSizeMb.toLong() * 1024 * 1024
        val skipHidden = exclusions.any { it.type == ExclusionType.HIDDEN }
        val candidates = mutableListOf<File>()
        var unreadable = 0
        for (folderPath in folders) {
            val root = File(folderPath)
            if (!root.exists()) continue
            if (root.listFiles() == null) {
                unreadable++
                SafeLog.w(TAG, "Backup folder cannot be read: $folderPath")
                continue
            }
            root.walkTopDown()
                .onEnter { dir -> dir == root || !skipHidden || !isHiddenName(dir) }
                .filter { it.isFile && it.length() > 0 }
                .forEach { candidates.add(it) }
        }

        if (candidates.isEmpty() && (unreadable > 0 || storageUnreadable())) {
            return AppResult.Failure(AppError.BackupFoldersUnreadable)
        }

        var totalBytes = 0L
        var skipped = 0
        val reasons = mutableMapOf<BackupDecision, Int>()
        val toBackup = mutableListOf<File>()
        for (candidate in candidates) {
            if (backupDao.recordByPath(candidate.absolutePath, activeChatId) == null &&
                (adoptLinkedUpload(candidate, activeChatId) ||
                        reviveTrashedUpload(candidate, activeChatId) ||
                        adoptUploadedCopy(candidate, activeChatId))
            ) {
                skipped++
                continue
            }
            val record = backupDao.recordByPath(candidate.absolutePath, activeChatId)
            val mime = MimeTypes.fromFileName(candidate.name)
            val decision = decideBackupAction(
                candidate = ExclusionCandidate(
                    absolutePath = candidate.absolutePath,
                    sizeBytes = candidate.length(),
                    mimeType = mime,
                    isHidden = candidate.name.startsWith('.')
                ),
                modifiedAt = candidate.lastModified(),
                existingRecord = record?.let {
                    ExistingBackupRecord(
                        it.sizeBytes, it.modifiedAt, it.contentHash
                    )
                },
                exclusions = exclusions,
                maxFileSizeBytes = maxSizeBytes,
                contentHashProvider = { Hashing.sha256(candidate) }
            )
            if (decision == BackupDecision.BACKUP) {
                toBackup.add(candidate)
                totalBytes += candidate.length()
            } else {
                skipped++
                reasons[decision] = (reasons[decision] ?: 0) + 1
            }
        }

        if (toBackup.isEmpty()) {
            SafeLog.d(
                TAG,
                "Backup scan: nothing to do, $skipped of ${candidates.size} skipped, " +
                        "reasons=$reasons"
            )
            return AppResult.Success(null)
        }

        val session = BackupSessionEntity(
            id = UUID.randomUUID().toString(),
            trigger = trigger,
            status = BackupSessionStatus.RUNNING,
            totalFiles = toBackup.size,
            skippedFiles = skipped,
            totalBytes = totalBytes,
            startedAt = System.currentTimeMillis()
        )
        backupDao.upsertSession(session)

        for (batch in toBackup.chunked(ENQUEUE_BATCH)) {
            val fileIds = batch.map {
                registerFile(it, backupFolderIdFor(it, activeChatId), activeChatId)
            }
            transferRepository.enqueueBackupBatch(fileIds, session.id)
        }
        backupSessionTracker.refresh(session.id)
        if (backupDao.sessionById(session.id)?.status == BackupSessionStatus.CANCELLED) {
            return AppResult.Success(null)
        }
        return AppResult.Success(session.id)
    }

    /**
     * A file uploaded by hand already has a remote mapping, so adding its folder to backup claims
     * it instead.
     */
    private suspend fun adoptLinkedUpload(candidate: File, chatId: Long?): Boolean {
        val existing = fileDao.byLocalPath(candidate.absolutePath, chatId) ?: return false
        if (existing.messageId == null || existing.backupState != BackupState.BACKED_UP) {
            return false
        }
        recordBackedUp(candidate, existing.id, existing.contentHash, chatId)
        SafeLog.d(TAG, "Claimed a manual upload already stored at this path")
        return true
    }

    /** A file trashed in the app but still on disk is restored instead of uploaded again. */
    private suspend fun reviveTrashedUpload(candidate: File, chatId: Long?): Boolean {
        val revived = fileRepository.reviveTrashedCopy(
            localPath = candidate.absolutePath,
            folderId = backupFolderIdFor(candidate, chatId)
        ) ?: return false
        if (!revived.hasRemoteCopy) return false
        recordBackedUp(candidate, revived.id, revived.contentHash, chatId)
        SafeLog.d(TAG, "Restored a trashed file instead of uploading a copy")
        return true
    }

    /**
     * After a reinstall uploaded files have no local path; matching the identical file avoids a
     * second upload.
     */
    private suspend fun adoptUploadedCopy(candidate: File, chatId: Long?): Boolean {
        val matches = fileDao.unlinkedRemoteMatches(candidate.name, candidate.length(), chatId)
        if (matches.isEmpty()) return false

        val localHash = Hashing.sha256(candidate)
        val match = matches.firstOrNull { it.contentHash != null && it.contentHash == localHash }
            ?: matches.singleOrNull()?.takeIf { it.contentHash == null }
            ?: return false

        fileDao.setLocalPath(match.id, candidate.absolutePath)
        recordBackedUp(candidate, match.id, localHash, chatId)
        SafeLog.d(TAG, "Linked an existing upload to its local copy")
        return true
    }

    private suspend fun recordBackedUp(
        candidate: File,
        fileId: String,
        contentHash: String?,
        chatId: Long?
    ) {
        backupDao.upsertRecord(
            BackupRecordEntity(
                id = UUID.randomUUID().toString(),
                sourcePath = candidate.absolutePath,
                fileId = fileId,
                sizeBytes = candidate.length(),
                modifiedAt = candidate.lastModified(),
                contentHash = contentHash,
                backedUpAt = System.currentTimeMillis(),
                chatId = chatId
            )
        )
    }

    /** Backed-up files share the drive tree with manual uploads instead of a separate silo. */
    private suspend fun backupFolderIdFor(source: File, chatId: Long?): String? {
        val parent = source.parentFile?.absolutePath ?: return null
        val relative = relativeToStorageRoot(parent)
        return if (relative.isEmpty()) {
            null
        } else {
            folderPathResolver.resolveOrCreate(relative, chatId = chatId)
        }
    }

    private fun relativeToStorageRoot(absolutePath: String): String {
        val normalized = absolutePath.trimEnd('/')
        val primary = storagePaths.externalStorageRoot?.absolutePath?.trimEnd('/')
        if (primary != null) {
            if (normalized == primary) return ""
            if (normalized.startsWith("$primary/")) return normalized.removePrefix("$primary/")
        }
        if (normalized.startsWith(VOLUME_MOUNT_ROOT)) {
            val rest = normalized.removePrefix(VOLUME_MOUNT_ROOT)
            val separator = rest.indexOf('/')
            return if (separator >= 0) rest.substring(separator + 1) else ""
        }
        return normalized.trimStart('/')
    }

    /**
     * Refreshes size, timestamp and hash first, or the backup record is stale and every later scan
     * uploads again.
     */
    private suspend fun registerFile(
        source: File,
        folderId: String?,
        chatId: Long?
    ): String {
        fileDao.byLocalPath(source.absolutePath, chatId)?.let { existing ->
            val changed = existing.sizeBytes != source.length() ||
                    existing.modifiedAt != source.lastModified()
            if (changed) {
                val media = mediaMetadataExtractor.extract(
                    source, MimeTypes.fromFileName(source.name)
                )
                fileDao.upsert(
                    existing.copy(
                        sizeBytes = source.length(),
                        modifiedAt = source.lastModified()
                            .takeIf { it > 0 } ?: existing.modifiedAt,
                        contentHash = null,
                        width = media.width,
                        height = media.height,
                        durationMs = media.durationMs
                    )
                )
            }
            return existing.id
        }

        val mime = MimeTypes.fromFileName(source.name)
        val media = mediaMetadataExtractor.extract(source, mime)
        val now = System.currentTimeMillis()
        val entity = FileEntity(
            id = UUID.randomUUID().toString(),
            chatId = chatId,
            folderId = folderId,
            name = source.name,
            sizeBytes = source.length(),
            mimeType = mime,
            category = FileCategory.fromMimeType(mime),
            localPath = source.absolutePath,
            contentHash = null,
            messageId = null,
            remoteFileId = null,
            remoteUniqueId = null,
            backupState = BackupState.QUEUED,
            width = media.width,
            height = media.height,
            durationMs = media.durationMs,
            createdAt = source.lastModified().takeIf { it > 0 } ?: now,
            modifiedAt = source.lastModified().takeIf { it > 0 } ?: now,
            addedAt = now
        )
        fileDao.upsert(entity)
        return entity.id
    }

    override suspend fun pauseBackup(sessionId: String) {
        setTransfers(sessionId, from = TransferState.QUEUED, to = TransferState.PAUSED)
        setTransfers(sessionId, from = TransferState.RUNNING, to = TransferState.PAUSED)
        backupDao.setSessionStatus(sessionId, BackupSessionStatus.PAUSED, null)
    }

    override suspend fun resumeBackup(sessionId: String) {
        setTransfers(sessionId, from = TransferState.PAUSED, to = TransferState.QUEUED)
        backupDao.setSessionStatus(sessionId, BackupSessionStatus.RUNNING, null)
        transferRepository.recoverOrphanedTransfers()
    }

    override suspend fun cancelBackup(sessionId: String) {
        for (transfer in transferDao.bySession(sessionId)) {
            if (!transfer.state.isTerminal) {
                cancelTransfer(transfer)
            }
        }
        fileDao.deleteCancelledBackupEntries()
        backupDao.setSessionStatus(
            sessionId,
            BackupSessionStatus.CANCELLED,
            System.currentTimeMillis()
        )
    }

    override suspend fun syncActiveSessionWithSelection() {
        val session = backupDao.activeSession() ?: return
        val folders = settingsRepository.preferences.first().storageChatId
            ?.let { channelRepository.backupFolders(it) }
            .orEmpty()
        var dropped = 0
        for (transfer in transferDao.bySession(session.id)) {
            if (transfer.state.isTerminal) continue
            val sourcePath = transfer.fileId?.let { fileDao.byId(it)?.localPath } ?: continue
            if (folders.none { isInsideFolder(sourcePath, it) }) {
                cancelTransfer(transfer)
                dropped++
            }
        }
        if (dropped > 0) {
            fileDao.deleteCancelledBackupEntries()
            backupSessionTracker.refresh(session.id)
        }
    }

    private suspend fun cancelTransfer(transfer: TransferEntity) {
        transferDao.setState(transfer.id, TransferState.CANCELLED, System.currentTimeMillis())
        transfer.fileId?.let { fileDao.setBackupStateIfLocalOnly(it, BackupState.NONE) }
    }

    private fun isInsideFolder(path: String, folder: String): Boolean {
        val root = folder.trimEnd('/')
        return path == root || path.startsWith("$root/")
    }

    private fun isHiddenPath(path: String, folder: String): Boolean {
        val root = folder.trimEnd('/')
        return path.removePrefix(root).trimStart('/').split('/').any { it.startsWith('.') }
    }

    override suspend fun refreshActiveSession() {
        backupDao.activeSession()?.let { backupSessionTracker.refresh(it.id) }
    }

    override suspend fun scanFolderChanges(): AppResult<List<FolderChange>> =
        withContext(dispatchers.io) { scanChanges() }

    private suspend fun scanChanges(): AppResult<List<FolderChange>> {
        val prefs = settingsRepository.preferences.first()
        val chatId = prefs.storageChatId
        val folders = chatId?.let { channelRepository.backupFolders(it) }.orEmpty()
        if (folders.isEmpty()) return AppResult.Success(emptyList())

        val exclusions = exclusionRepository.getEnabled()
        val maxSizeBytes = prefs.backupMaxFileSizeMb.toLong() * 1024 * 1024
        val skipHidden = exclusions.any { it.type == ExclusionType.HIDDEN }
        val recordsByPath = backupDao.recordsInChat(chatId).associateBy { it.sourcePath }

        val changes = mutableListOf<FolderChange>()
        for (folderPath in folders) {
            val root = File(folderPath)
            if (!root.exists() || root.listFiles() == null) continue

            var added = 0
            var changed = 0
            val seenPaths = mutableSetOf<String>()
            root.walkTopDown()
                .onEnter { dir -> dir == root || !skipHidden || !isHiddenName(dir) }
                .filter { it.isFile && it.length() > 0 }
                .forEach { candidate ->
                    seenPaths.add(candidate.absolutePath)
                    val record = recordsByPath[candidate.absolutePath]
                    // A file uploaded by hand already has a remote copy; syncing just claims it.
                    if (record == null && isAlreadyUploaded(candidate.absolutePath, chatId)) {
                        return@forEach
                    }
                    val decision = decideBackupAction(
                        candidate = ExclusionCandidate(
                            absolutePath = candidate.absolutePath,
                            sizeBytes = candidate.length(),
                            mimeType = MimeTypes.fromFileName(candidate.name),
                            isHidden = candidate.name.startsWith('.')
                        ),
                        modifiedAt = candidate.lastModified(),
                        existingRecord = record?.let {
                            ExistingBackupRecord(it.sizeBytes, it.modifiedAt, it.contentHash)
                        },
                        exclusions = exclusions,
                        maxFileSizeBytes = maxSizeBytes,
                        contentHashProvider = { Hashing.sha256(candidate) }
                    )
                    if (decision == BackupDecision.BACKUP) {
                        if (record == null) added++ else changed++
                    }
                }

            var deleted = 0
            for (record in recordsByPath.values) {
                if (!isInsideFolder(record.sourcePath, folderPath)) continue
                if (seenPaths.contains(record.sourcePath)) continue
                // Hidden files were never visited, so their absence proves nothing.
                if (skipHidden && isHiddenPath(record.sourcePath, folderPath)) continue
                if (removedFile(record) != null) deleted++
            }

            if (added + changed + deleted > 0) {
                changes.add(FolderChange(folderPath, added, changed, deleted))
            }
        }
        return AppResult.Success(changes)
    }

    override suspend fun applyFolderChanges(folderPaths: Set<String>): AppResult<String?> =
        withContext(dispatchers.io) {
            if (folderPaths.isEmpty()) return@withContext AppResult.Success(null)

            val chatId = settingsRepository.preferences.first().storageChatId
            val selected = chatId?.let { channelRepository.backupFolders(it) }.orEmpty()
                .filter { it in folderPaths }
                .toSet()
            if (selected.isEmpty()) return@withContext AppResult.Success(null)

            removeDeletedFiles(selected, chatId)
            runBackup(BackupTrigger.MANUAL, selected)
        }

    /**
     * Deleted local files leave the drive by way of the trash so the sync stays recoverable.
     * Entries the app cleaned up itself, or that are already trashed, are not touched remotely.
     */
    private suspend fun removeDeletedFiles(folders: Set<String>, chatId: Long?) {
        val toTrash = mutableListOf<String>()
        val staleRecordIds = mutableListOf<String>()
        for (record in backupDao.recordsInChat(chatId)) {
            if (folders.none { isInsideFolder(record.sourcePath, it) }) continue
            if (!isDeletedFromDisk(record.sourcePath)) continue

            val file = record.fileId?.let { fileDao.byId(it) }
            if (file == null || file.trashedAt != null) {
                staleRecordIds.add(record.id)
                continue
            }
            // Null localPath means the app released the copy (delete-after-upload), not a deletion.
            if (file.localPath == null || file.localPath != record.sourcePath) continue
            if (file.messageId == null) {
                staleRecordIds.add(record.id)
                continue
            }
            toTrash.add(file.id)
            staleRecordIds.add(record.id)
        }

        if (toTrash.isNotEmpty()) trashRepository.moveFilesToTrash(toTrash)
        if (staleRecordIds.isNotEmpty()) backupDao.deleteRecordsByIds(staleRecordIds)
    }

    private suspend fun removedFile(record: BackupRecordEntity): FileEntity? {
        val file = record.fileId?.let { fileDao.byId(it) } ?: return null
        if (file.trashedAt != null) return null
        val localPath = file.localPath ?: return null
        if (localPath != record.sourcePath) return null
        if (File(localPath).exists()) return null
        return file.takeIf { it.messageId != null }
    }

    /**
     * Only counts a missing file as deleted when the nearest existing ancestor is readable, so an
     * unmounted SD card or a denied permission cannot wipe the drive.
     */
    private fun isDeletedFromDisk(path: String): Boolean {
        val file = File(path)
        if (file.exists()) return false
        var ancestor = file.parentFile
        while (ancestor != null && !ancestor.exists()) {
            ancestor = ancestor.parentFile
        }
        return ancestor != null && ancestor.listFiles() != null
    }

    private suspend fun isAlreadyUploaded(path: String, chatId: Long?): Boolean {
        val existing = fileDao.byLocalPath(path, chatId) ?: return false
        return existing.messageId != null && existing.backupState == BackupState.BACKED_UP
    }

    private suspend fun setTransfers(sessionId: String, from: TransferState, to: TransferState) {
        for (transfer in transferDao.bySession(sessionId)) {
            if (transfer.state == from) {
                transferDao.setState(transfer.id, to, System.currentTimeMillis())
            }
        }
    }

    /**
     * A marker like .nomedia says how a gallery indexes a folder, not whether its contents matter,
     * so only the item itself is hidden.
     */
    private fun isHiddenName(file: File): Boolean = file.name.startsWith('.')

    private fun storageUnreadable(): Boolean {
        val root = storagePaths.externalStorageRoot ?: return false
        return root.exists() && root.listFiles() == null
    }

    companion object {
        private const val TAG = "BackupRepository"
        private const val ENQUEUE_BATCH = 500
        private const val VOLUME_MOUNT_ROOT = "/storage/"
    }
}
