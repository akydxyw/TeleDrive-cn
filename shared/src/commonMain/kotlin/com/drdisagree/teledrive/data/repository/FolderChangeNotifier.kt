package com.drdisagree.teledrive.data.repository

import com.drdisagree.teledrive.domain.model.FolderChange
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * Carries folder changes found anywhere in the app to the one sync dialog shown by the app shell.
 */
class FolderChangeNotifier {

    private val _pending = MutableStateFlow<List<FolderChange>>(emptyList())
    val pending: StateFlow<List<FolderChange>> = _pending.asStateFlow()

    fun publish(changes: List<FolderChange>) {
        _pending.value = changes
    }

    fun clear() {
        _pending.value = emptyList()
    }
}
