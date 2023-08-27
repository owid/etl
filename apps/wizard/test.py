from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from datetime import date
from typing import Callable, TypeVar

import streamlit as st

T = TypeVar("T")


@dataclass
class Test:
    a: int
    b: str = "some value"
    c: date = field(default_factory=date.today)

    def validate(self) -> dict[str, str]:
        """Sample validation for the demo. Use your preferred framework instead."""
        result = {}
        if self.c >= date.today():
            result["c"] = "date must be in the past"
        if self.b == "some value":
            result["b"] = "please, change the value"
        return result


print("START")
_FORM_COMPONENT_KEY = "dc_form_component"
_FORM_VALIDATION_KEY = "dc_form_validation"


def show_dataclass_form(obj_or_type: T | type[T], action: Callable[[T], None], key: str = "") -> None:
    """Show dataclass edit form with single button, which triggers action.
    Args:
        obj_or_type (T | type[T]): _description_
        action (Callable[[T],None]): _description_
        key (str, optional): _description_. Defaults to "".
    """
    assert is_dataclass(obj_or_type), f"{obj_or_type} is not a dataclass"
    # pop previous run validation results
    validation_results = st.session_state.pop(key + _FORM_VALIDATION_KEY, {})
    # determine base type
    base_type: type = obj_or_type if isinstance(obj_or_type, type) else type(obj_or_type)
    # show fields
    result = {}
    with st.form(key=key + _FORM_COMPONENT_KEY):
        for field in fields(base_type):
            # get field value
            value = getattr(obj_or_type, field.name, None)
            if value in (MISSING, None):
                if field.default is not MISSING:
                    value = field.default
                elif field.default_factory is not MISSING:
                    value = field.default_factory()
                else:
                    value = field.type()

            # show field
            field_key = key + _FORM_COMPONENT_KEY + field.name
            if field.type is str:
                result[field.name] = st.text_input(field.name + ":", value, key=field_key)
            elif field.type is int:
                result[field.name] = st.number_input(field.name + ":", value, key=field_key)
            elif field.type is date:
                result[field.name] = st.date_input(field.name + ":", value, key=field_key)
            # etc, for all wanted types

            # show validation messages
            if field.name in validation_results:
                print(f"3: ERR FOR {field.name}")
                st.warning("**Validation error:** " + validation_results[field.name])
            else:
                print(f"3: OK FOR {field.name}")
        if st.form_submit_button("Confirm"):
            validation_results = {}
            try:
                print("4: SUBMIT_OK")
                new_object = base_type(**result)
                if getattr(base_type, "validate", None):
                    validation_results = new_object.validate()
            except Exception as e:
                print("4: SUBMIT_ERR")
                # CATCH VALIDATION ERROR AND POPULATE THE DICT WITH IT
                pass
            if validation_results:
                st.session_state[key + _FORM_VALIDATION_KEY] = validation_results
                print("5: RERUN")
                st.experimental_rerun()
            else:
                print("5: ACTION")
                st.write(result)
                # action(new_object)


def show_dataclass_as_json(obj) -> None:
    st.json(obj.__dict__)


if __name__ == "__main__":
    show_dataclass_form(Test, show_dataclass_as_json)
