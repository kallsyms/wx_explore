from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint
from wx_explore.web import db


def get_or_create(obj):
    """
    Gets the specified object from the DB using the values of primary and unique keys for lookup,
    or creates the object in the database and returns it.
    """
    typ = type(obj)
    q = db.session.query(typ)
    constraints = [c for c in typ.__table__.constraints if isinstance(c, (PrimaryKeyConstraint, UniqueConstraint))]
    for col in typ.__table__.columns:
        if getattr(obj, col.name) is not None:
            if any(ccol is col for constraint in constraints for ccol in constraint.columns):
                q = q.filter(getattr(typ, col.name) == getattr(obj, col.name))

    instance = q.first()
    if instance is not None:
        return instance
    else:
        db.session.add(obj)
        # Commit so the result is guaranteed to have an id if applicable
        db.session.commit()
        return obj
