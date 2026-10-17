package com.drdisagree.teledrive.data.local.entity

/** Lightweight view of a remote file, enough to tell whether a local copy still exists. */
data class RemoteFileRef(
    val id: String,
    val name: String,
    val localPath: String?
)
