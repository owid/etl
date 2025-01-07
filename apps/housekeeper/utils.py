from sqlalchemy.orm import Session

from etl.config import OWID_ENV
from etl.grapher import model as gm


def get_reviews_id(object_type: str):
    with Session(OWID_ENV.engine) as session:
        return gm.HousekeepingSuggestedReview.load_reviews_object_id(session, object_type=object_type)


def add_reviews(object_type: str, object_id: int):
    with Session(OWID_ENV.engine) as session:
        gm.HousekeepingSuggestedReview.add_review(
            session=session,
            object_type=object_type,
            object_id=object_id,
        )
