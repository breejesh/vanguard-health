"""Prune Firestore gold_conditions to an allowed list of condition codes."""

from firebase_admin import firestore

from src.common.firebase_pusher import init_firestore_client

ALLOWED_CONDITION_CODES = {"840539006", "840544004"}
BATCH_LIMIT = 450


def main() -> int:
    db = init_firestore_client()
    docs = list(db.collection("gold_conditions").stream())

    batch = db.batch()
    pending = 0
    deleted = 0

    for doc in docs:
        condition_code = str(doc.id)
        if condition_code in ALLOWED_CONDITION_CODES:
            continue

        batch.delete(doc.reference)
        pending += 1
        deleted += 1

        if pending >= BATCH_LIMIT:
            batch.commit()
            batch = db.batch()
            pending = 0

    if pending:
        batch.commit()

    db.collection("gold_overview").document("current").set(
        {
            "condition_codes": sorted(ALLOWED_CONDITION_CODES),
            "total_conditions": len(ALLOWED_CONDITION_CODES),
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    print(f"gold_conditions scanned={len(docs)} deleted={deleted} kept={len(docs) - deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
