package com.drdisagree.teledrive.domain.model

/** Local changes found in a backup folder since the last sync. */
data class FolderChange(
    val folderPath: String,
    val addedFiles: Int,
    val changedFiles: Int,
    val deletedFiles: Int
) {
    val folderName: String
        get() = folderPath.trimEnd('/').substringAfterLast('/').ifBlank { folderPath }

    val totalChanges: Int
        get() = addedFiles + changedFiles + deletedFiles
}
