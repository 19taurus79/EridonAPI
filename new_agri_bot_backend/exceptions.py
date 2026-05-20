class ExcelValidationError(Exception):
    def __init__(self, file_type: str, message: str, missing_columns: list = None):
        self.file_type = file_type  # 'ordered' (Заказано) или 'moved' (Перемещено)
        self.message = message
        self.missing_columns = missing_columns or []
        super().__init__(message)
