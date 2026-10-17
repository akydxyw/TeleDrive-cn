package com.drdisagree.teledrive.presentation.components

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.selection.toggleable
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.Checkbox
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.unit.dp
import com.drdisagree.teledrive.domain.model.FolderChange
import com.drdisagree.teledrive.resources.Res
import com.drdisagree.teledrive.resources.folder_changes_added
import com.drdisagree.teledrive.resources.folder_changes_changed
import com.drdisagree.teledrive.resources.folder_changes_deleted
import com.drdisagree.teledrive.resources.folder_changes_later
import com.drdisagree.teledrive.resources.folder_changes_message
import com.drdisagree.teledrive.resources.folder_changes_sync_selected
import com.drdisagree.teledrive.resources.folder_changes_title
import org.jetbrains.compose.resources.stringResource

/** Lists the backup folders that changed while the app was closed, all ticked by default. */
@Composable
fun FolderChangeSyncDialog(
    changes: List<FolderChange>,
    onSync: (Set<String>) -> Unit,
    onDismiss: () -> Unit
) {
    var selected by remember(changes) {
        mutableStateOf(changes.map { it.folderPath }.toSet())
    }

    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(Res.string.folder_changes_title)) },
        text = {
            Column {
                Text(
                    text = stringResource(Res.string.folder_changes_message),
                    style = MaterialTheme.typography.bodyMedium
                )
                Column(
                    modifier = Modifier
                        .heightIn(max = 320.dp)
                        .verticalScroll(rememberScrollState())
                ) {
                    changes.forEach { change ->
                        FolderChangeRow(
                            change = change,
                            checked = change.folderPath in selected,
                            onToggle = { checked ->
                                selected = if (checked) {
                                    selected + change.folderPath
                                } else {
                                    selected - change.folderPath
                                }
                            }
                        )
                    }
                }
            }
        },
        confirmButton = {
            Button(
                onClick = { onSync(selected) },
                enabled = selected.isNotEmpty()
            ) {
                Text(stringResource(Res.string.folder_changes_sync_selected))
            }
        },
        dismissButton = {
            TextButton(onClick = onDismiss) {
                Text(stringResource(Res.string.folder_changes_later))
            }
        }
    )
}

@Composable
private fun FolderChangeRow(
    change: FolderChange,
    checked: Boolean,
    onToggle: (Boolean) -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .toggleable(value = checked, role = Role.Checkbox, onValueChange = onToggle)
            .padding(vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Checkbox(checked = checked, onCheckedChange = null)
        Column(Modifier.padding(start = 8.dp)) {
            Text(
                text = change.folderName,
                style = MaterialTheme.typography.bodyLarge
            )
            Text(
                text = change.folderPath,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            Text(
                text = changeSummary(change),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
private fun changeSummary(change: FolderChange): String = listOfNotNull(
    change.addedFiles.takeIf { it > 0 }
        ?.let { stringResource(Res.string.folder_changes_added, it) },
    change.changedFiles.takeIf { it > 0 }
        ?.let { stringResource(Res.string.folder_changes_changed, it) },
    change.deletedFiles.takeIf { it > 0 }
        ?.let { stringResource(Res.string.folder_changes_deleted, it) }
).joinToString(" · ")
