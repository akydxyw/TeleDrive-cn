package com.drdisagree.teledrive.core.telegram

/** Never logged. */
data class TelegramCredentials(
    val apiId: Int,
    val apiHash: String
) {
    override fun toString(): String = "TelegramCredentials(apiId=***, apiHash=***)"
}
