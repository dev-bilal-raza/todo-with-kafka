from sqlmodel import Field, SQLModel


class TOdo(SQLModel, table=True):
    id: int | None = Field(int, primary_key=True)
    todo_name: str
    todo_status: bool = False
