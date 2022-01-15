from m_config import S_TABLE_COLUMN_LIMIT, S_TABLE_LIMIT, S_TABLE_ROW_LIMIT


class MTab_Exception(Exception):
    def __init__(self, e_id, e_message):
        self.id = e_id
        self.message = e_message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.id:s}: {self.message:s}"


class M_E_Limit_Reached(MTab_Exception):
    def __init__(self, e_message=None):
        if not e_message:
            e_message = f"We could only process up to {S_TABLE_LIMIT} tables, Each table must has less than {S_TABLE_COLUMN_LIMIT} columns and {S_TABLE_ROW_LIMIT} rows."
        self.message = e_message
        self.id = "ERR_LIMIT_REACHED"
        super().__init__(self.id, self.message)


class M_E_Table_Invalid_Format(MTab_Exception):
    def __init__(self, e_message=None):
        if not e_message:
            e_message = "Table Format Invalid. Please re-check your input tables"
        self.message = e_message
        self.id = "ERR_TABLE_INVALID_FORMAT"
        super().__init__(self.id, self.message)


class M_E_Target_Invalid_Format(MTab_Exception):
    def __init__(self, e_message=None):
        if not e_message:
            e_message = (
                "Target Format Invalid. Please re-check your the target matching"
            )
        self.message = e_message
        self.id = "ERR_TARGET_INVALID_FORMAT"
        super().__init__(self.id, self.message)
