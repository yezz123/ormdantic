"""Forward and rollback operation list."""

from __future__ import annotations

from textual.widgets import Label, ListItem, ListView

from ormdantic.playground.workspace import ArtifactDocument


class OperationItem(ListItem):
    """One selectable forward or rollback SQL operation."""

    def __init__(
        self,
        label: str,
        *,
        index: int,
        rollback: bool,
    ) -> None:
        super().__init__(Label(label))
        self.operation_index = index
        self.rollback = rollback


class OperationList(ListView):
    """Operations grouped by direction with stable index metadata."""

    def update_document(self, document: ArtifactDocument | None) -> None:
        self.remove_children()
        if document is None or document.artifact is None:
            self.index = None
            return
        items: list[OperationItem] = []
        for index, operation in enumerate(document.artifact.operations):
            items.append(
                OperationItem(
                    f"UP {index + 1:02d}  {operation.kind}",
                    index=index,
                    rollback=False,
                )
            )
        for index, operation in enumerate(document.artifact.rollback_operations):
            items.append(
                OperationItem(
                    f"DOWN {index + 1:02d}  {operation.kind}",
                    index=index,
                    rollback=True,
                )
            )
        if items:
            self.mount(*items)
            self.index = 0
        else:
            self.index = None
