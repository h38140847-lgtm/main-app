# ═══════════════════════════════════════════════════════════════════════════════
#  app.py  —  FreshMart Backend
#
#  CHANGES vs previous version:
#    • add-product    → now saves stockUnit field
#    • update-product → now updates stockUnit field
#    • get-products   → stockUnit is returned in product dict (auto, no change needed)
#
#  FIELD MEANINGS:
#    price      — selling price in ₹ (e.g. 40)
#    unitValue  — quantity in one pack (e.g. 500)
#    unitType   — unit of one pack (e.g. "g")  → together: "₹40 per 500g"
#    quantity   — stock amount number (e.g. 25)
#    stockUnit  — unit of stock (e.g. "kg")    → together: "25 kg in stock"
#
#  ORDER GATE:
#    • Customer registers → approved=False by default.
#    • Owner flips the toggle in Customers page → approved=True.
#    • place-order checks approved flag. If False → 403.
#
#  DELIVERY BEHAVIOUR:
#    • When owner sets order status to "Delivered" → order is permanently
#      deleted from the database. Stock is NOT restored.
# ═══════════════════════════════════════════════════════════════════════════════
import base64
import json
import logging
import math
import os
import uuid
from datetime import datetime, UTC

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.security import check_password_hash, generate_password_hash

import firebase_admin
from firebase_admin import credentials, firestore, messaging
from google.cloud.firestore import ArrayUnion, ArrayRemove
from google.cloud.firestore_v1.base_query import FieldFilter

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("freshmart")


# ─────────────────────────────────────────────────────────────────────────────
# FIREBASE INIT
# ─────────────────────────────────────────────────────────────────────────────
if not firebase_admin._apps:
    firebase_config = os.environ.get("FIREBASE_KEY")
    if firebase_config:
        try:
            # Railway env var can be raw JSON or base64-encoded JSON.
            cred_data = json.loads(firebase_config)
        except json.JSONDecodeError:
            decoded = base64.b64decode(firebase_config).decode("utf-8")
            cred_data = json.loads(decoded)
        cred = credentials.Certificate(cred_data)
    else:
        cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()
FCM_ANDROID_CHANNEL_ID = (os.environ.get("FCM_ANDROID_CHANNEL_ID") or "freshmart_orders").strip()
FCM_ANDROID_CLICK_ACTION = (os.environ.get("FCM_ANDROID_CLICK_ACTION") or "FCM_PLUGIN_ACTIVITY").strip()
WEBPUSH_CLICK_LINK = (os.environ.get("WEBPUSH_CLICK_LINK") or "").strip()


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _order_dict(doc):
    d = doc.to_dict()
    d["id"] = doc.id
    for key in ("createdAt", "updatedAt"):
        if key in d and hasattr(d[key], "isoformat"):
            d[key] = d[key].isoformat()
    return d


def _message_data(data: dict | None = None) -> dict:
    return {str(k): str(v) for k, v in (data or {}).items()}


def _is_https_url(url: str) -> bool:
    return url.startswith("https://")


def _build_fcm_message(token: str, title: str, body: str, data: dict | None = None):
    android_notif = messaging.AndroidNotification(
        sound="default",
        click_action=FCM_ANDROID_CLICK_ACTION,
    )
    if FCM_ANDROID_CHANNEL_ID:
        android_notif.channel_id = FCM_ANDROID_CHANNEL_ID

    webpush_cfg = messaging.WebpushConfig(
        headers={"Urgency": "high"},
        notification=messaging.WebpushNotification(
            title=title,
            body=body,
            icon="/logo192.png",
            badge="/logo192.png",
            tag="freshmart-order",
            require_interaction=True,
        ),
    )
    if _is_https_url(WEBPUSH_CLICK_LINK):
        webpush_cfg.fcm_options = messaging.WebpushFCMOptions(link=WEBPUSH_CLICK_LINK)

    return messaging.Message(
        notification=messaging.Notification(title=title, body=body),
        data=_message_data(data),
        android=messaging.AndroidConfig(
            priority="high",
            ttl=120,
            notification=android_notif,
        ),
        apns=messaging.APNSConfig(
            payload=messaging.APNSPayload(
                aps=messaging.Aps(sound="default", badge=1)
            )
        ),
        webpush=webpush_cfg,
        token=token,
    )


def _collect_tokens(doc_dict: dict) -> list[str]:
    tokens: list[str] = []
    if isinstance(doc_dict.get("fcmTokens"), list):
        tokens.extend([t for t in doc_dict.get("fcmTokens") if t])
    if doc_dict.get("fcmToken"):
        tokens.append(doc_dict.get("fcmToken"))
    # De-duplicate while preserving order
    return list(dict.fromkeys(tokens))


def send_push(title: str, body: str, data: dict = None):
    """Send FCM push to ALL owner tokens stored in the owners collection."""
    sent = 0
    invalid_tokens = 0
    for owner_doc in db.collection("owners").stream():
        tokens = _collect_tokens(owner_doc.to_dict())
        if not tokens:
            continue

        for token in tokens:
            try:
                msg = _build_fcm_message(token=token, title=title, body=body, data=data)
                messaging.send(msg)
                sent += 1
            except Exception as e:
                err = str(e).lower()
                logger.warning("[FCM] send failed for token %s...: %s", token[:20], err)
                if (
                    "registration-token-not-registered" in err
                    or "requested entity was not found" in err
                    or "unregistered" in err
                ):
                    owner_doc.reference.update({
                        "fcmTokens": ArrayRemove([token]),
                        "fcmToken": firestore.DELETE_FIELD,
                    })
                    invalid_tokens += 1
    logger.info("[FCM] Sent to %s token(s), removed %s invalid token(s): %s", sent, invalid_tokens, title)
    return sent


def send_delivery_push(mobile: str, title: str, body: str, data: dict = None) -> int:
    """Send FCM push to a single delivery boy (all registered tokens)."""
    if not mobile:
        return 0
    ref = db.collection("delivery_boys").document(mobile)
    doc = ref.get()
    if not doc.exists:
        return 0
    tokens = _collect_tokens(doc.to_dict())
    if not tokens:
        return 0
    sent = 0
    for token in tokens:
        try:
            msg = _build_fcm_message(token=token, title=title, body=body, data=data)
            messaging.send(msg)
            sent += 1
        except Exception as e:
            err = str(e).lower()
            logger.warning("[FCM] delivery send failed for token %s...: %s", token[:20], err)
            if (
                "registration-token-not-registered" in err
                or "requested entity was not found" in err
                or "unregistered" in err
            ):
                ref.update({
                    "fcmTokens": ArrayRemove([token]),
                    "fcmToken": firestore.DELETE_FIELD,
                })
    return sent

def send_customer_push(mobile: str, title: str, body: str, data: dict = None) -> int:
    """Send FCM push to a single customer (all registered tokens)."""
    sent = 0
    user_ref = db.collection("users").document(mobile)
    user_doc = user_ref.get()
    if not user_doc.exists:
        return 0
    tokens = _collect_tokens(user_doc.to_dict())
    if not tokens:
        return 0
    for token in tokens:
        try:
            msg = _build_fcm_message(token=token, title=title, body=body, data=data)
            messaging.send(msg)
            sent += 1
        except Exception as e:
            err = str(e).lower()
            logger.warning("[FCM] send failed for customer token %s...: %s", token[:20], err)
            if (
                "registration-token-not-registered" in err
                or "requested entity was not found" in err
                or "unregistered" in err
            ):
                user_ref.update({
                    "fcmTokens": ArrayRemove([token]),
                    "fcmToken": firestore.DELETE_FIELD,
                })
    return sent


def normalise_unit(unit_value, unit_type):
    """Convert kg→g and l→ml so the DB always stores the smaller unit."""
    try:
        v = float(unit_value)
    except (TypeError, ValueError):
        return unit_value, unit_type
    ut = _normalise_unit_str(unit_type)
    if ut == "kg":
        return v * 1000, "g"
    if ut == "l":
        return v * 1000, "ml"
    return v, ut


def _normalise_unit_str(unit):
    u = (unit or "").strip().lower()
    if u in ("pcs", "pc", "piece", "pieces", "pice", "pices"):
        return "pcs"
    if u in ("g", "gram", "grams"):
        return "g"
    if u in ("kg", "kgs", "kilogram", "kilograms"):
        return "kg"
    if u in ("ml", "milliliter", "milliliters", "millilitre", "millilitres"):
        return "ml"
    if u in ("l", "liter", "liters", "litre", "litres"):
        return "l"
    return u


def _verify_password(stored_value, plain_password):
    """Accept hashed passwords and legacy plaintext during migration."""
    if not stored_value:
        return False
    if stored_value.startswith("pbkdf2:"):
        return check_password_hash(stored_value, plain_password)
    return stored_value == plain_password


def _compute_delivery_charge(lat, lng, free_km, charge_amt):
    """Compute delivery charge from owner location."""
    if lat is None or lng is None or charge_amt <= 0:
        return 0.0

    for owner_doc in db.collection("owners").stream():
        od = owner_doc.to_dict()
        o_lat = od.get("latitude")
        o_lng = od.get("longitude")
        if o_lat is None or o_lng is None:
            break

        dlat = math.radians(float(o_lat) - float(lat))
        dlng = math.radians(float(o_lng) - float(lng))
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(float(lat)))
            * math.cos(math.radians(float(o_lat)))
            * math.sin(dlng / 2) ** 2
        )
        km = 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        if free_km > 0 and km <= free_km:
            return 0.0
        return round(km * charge_amt, 2)

    return 0.0
# ═══════════════════════════════
# OWNER — STORE SETTINGS
# ═══════════════════════════════
@app.route("/owner/settings", methods=["PUT"])
def update_store_settings():
    data = request.json or {}
    existing_doc = db.collection("store_settings").document("main").get()
    existing = existing_doc.to_dict() if existing_doc.exists else {}
    results = {}
    try:
        min_amt = float(data.get("minOrderAmount", existing.get("minOrderAmount", 0)))
        if min_amt < 0: raise ValueError()
        results["minOrderAmount"] = min_amt
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "minOrderAmount must be a non-negative number"}), 400
    try:
        free_km = float(data.get("freeDeliveryKm", existing.get("freeDeliveryKm", 0)))
        if free_km < 0: raise ValueError()
        results["freeDeliveryKm"] = free_km
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "freeDeliveryKm must be a non-negative number"}), 400
    try:
        charge = float(data.get("deliveryCharge", existing.get("deliveryCharge", 0)))
        if charge < 0: raise ValueError()
        results["deliveryCharge"] = charge
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "deliveryCharge must be a non-negative number"}), 400

    db.collection("store_settings").document("main").set({
        **results,
        "updatedAt": datetime.now(UTC),
    }, merge=True)
    return jsonify({"status": "success", **results})


def _restore_stock_for_order(order_data: dict):
    for item in order_data.get("items", []):
        pid = (item.get("productId") or "").strip()
        qty = float(item.get("qty", 0))
        if not pid or qty <= 0:
            continue
        prod_ref = db.collection("products").document(pid)
        prod_doc = prod_ref.get()
        if prod_doc.exists:
            prod_data  = prod_doc.to_dict()
            unit_value = float(prod_data.get("unitValue", 0))
            stock_unit = _normalise_unit_str(prod_data.get("stockUnit"))
            cur_stock  = float(prod_data.get("quantity", 0))

            total_base = qty * unit_value

            if stock_unit == "kg":
                restore = total_base / 1000
            elif stock_unit == "l":
                restore = total_base / 1000
            elif stock_unit in ("g", "ml", "pcs"):
                restore = total_base
            else:
                restore = total_base if stock_unit == _normalise_unit_str(prod_data.get("unitType")) else qty

            prod_ref.update({"quantity": cur_stock + restore})

# ═════════════════════════════════════════════════════════════════════════════
# AUTH — CUSTOMER
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/register", methods=["POST"])
def customer_register():
    data   = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    name   = (data.get("name") or "").strip()

    if not mobile or not name:
        return jsonify({"status": "error", "message": "Mobile and name are required"}), 400
    if len(mobile) != 10 or not mobile.isdigit():
        return jsonify({"status": "error", "message": "Enter a valid 10-digit mobile number"}), 400

    user_ref = db.collection("users").document(mobile)
    if user_ref.get().exists:
        return jsonify({"status": "error", "message": "Mobile number already registered. Please login."}), 409

    user_ref.set({
        "phone":     mobile,
        "name":      name,
        "role":      "customer",
        "approved":  False,
        "createdAt": datetime.now(UTC),
    })
    return jsonify({
        "status":  "success",
        "message": "Registered successfully",
        "user":    {"phone": mobile, "name": name},
    }), 201


@app.route("/login", methods=["POST"])
def customer_login():
    data   = request.json or {}
    mobile = (data.get("mobile") or "").strip()

    if not mobile or len(mobile) != 10 or not mobile.isdigit():
        return jsonify({"status": "error", "message": "Enter a valid 10-digit mobile number"}), 400

    user_ref = db.collection("users").document(mobile)
    user_doc = user_ref.get()
    if not user_doc.exists:
        return jsonify({"status": "error", "message": "Mobile not registered. Please register first."}), 403

    user_data = {k: v for k, v in user_doc.to_dict().items()
                 if k not in ("createdAt", "updatedAt")}
    return jsonify({"status": "success", "user": user_data})


# ═════════════════════════════════════════════════════════════════════════════
# AUTH — OWNER
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/owner/login", methods=["POST"])
def owner_login():
    data     = request.json or {}
    mobile   = (data.get("mobile") or "").strip()
    password = (data.get("password") or "").strip()

    owner_ref = db.collection("owners").document(mobile)
    owner_doc = owner_ref.get()
    if not owner_doc.exists:
        return jsonify({"status": "error", "message": "Owner not found"}), 404

    owner_data = owner_doc.to_dict()
    if not _verify_password(owner_data.get("password", ""), password):
        return jsonify({"status": "error", "message": "Invalid password"}), 401

    safe = {k: v for k, v in owner_data.items()
            if k not in ("password", "createdAt", "updatedAt")}
    return jsonify({"status": "success", "owner": safe})


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# FCM TOKEN â€” save per owner
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
@app.route("/owner/save-fcm-token", methods=["POST"])
def save_fcm_token():
    data = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    token = (data.get("fcmToken") or "").strip()

    if not mobile or not token:
        return jsonify({"status": "error", "message": "mobile and fcmToken required"}), 400

    ref = db.collection("owners").document(mobile)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Owner not found"}), 404

    ref.update({
        "fcmToken": token,
        "fcmTokens": ArrayUnion([token]),
        "tokenUpdatedAt": datetime.now(UTC),
    })
    logger.info("[FCM] Token saved for owner %s: %s...", mobile, token[:20])
    return jsonify({"status": "success", "message": "Token saved"})


@app.route("/owner/clear-fcm-token", methods=["POST"])
def clear_fcm_token():
    data = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    token = (data.get("fcmToken") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile required"}), 400
    ref = db.collection("owners").document(mobile)
    doc = ref.get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Owner not found"}), 404
    update_data = {"tokenUpdatedAt": datetime.now(UTC)}
    if token:
        update_data["fcmTokens"] = ArrayRemove([token])
        if (doc.to_dict() or {}).get("fcmToken") == token:
            update_data["fcmToken"] = firestore.DELETE_FIELD
        ref.update(update_data)
        logger.info("[FCM] Token removed for owner %s: %s...", mobile, token[:20])
        return jsonify({"status": "success", "message": "Token removed"})

    ref.update({
        "fcmToken": firestore.DELETE_FIELD,
        "fcmTokens": firestore.DELETE_FIELD,
        "tokenUpdatedAt": datetime.now(UTC),
    })
    logger.info("[FCM] All tokens cleared for owner %s", mobile)
    return jsonify({"status": "success", "message": "All tokens cleared"})


# CUSTOMER FCM TOKEN
@app.route("/customer/save-fcm-token", methods=["POST"])
def save_customer_fcm_token():
    data = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    token = (data.get("fcmToken") or "").strip()

    if not mobile or not token:
        return jsonify({"status": "error", "message": "mobile and fcmToken required"}), 400

    ref = db.collection("users").document(mobile)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Customer not found"}), 404

    ref.update({
        "fcmToken": token,
        "fcmTokens": ArrayUnion([token]),
        "tokenUpdatedAt": datetime.now(UTC),
    })
    logger.info("[FCM] Token saved for customer %s: %s...", mobile, token[:20])
    return jsonify({"status": "success", "message": "Token saved"})


@app.route("/customer/clear-fcm-token", methods=["POST"])
def clear_customer_fcm_token():
    data = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    token = (data.get("fcmToken") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile required"}), 400
    ref = db.collection("users").document(mobile)
    doc = ref.get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Customer not found"}), 404
    update_data = {"tokenUpdatedAt": datetime.now(UTC)}
    if token:
        update_data["fcmTokens"] = ArrayRemove([token])
        if (doc.to_dict() or {}).get("fcmToken") == token:
            update_data["fcmToken"] = firestore.DELETE_FIELD
        ref.update(update_data)
        logger.info("[FCM] Token removed for customer %s: %s...", mobile, token[:20])
        return jsonify({"status": "success", "message": "Token removed"})

    ref.update({
        "fcmToken": firestore.DELETE_FIELD,
        "fcmTokens": firestore.DELETE_FIELD,
        "tokenUpdatedAt": datetime.now(UTC),
    })
    logger.info("[FCM] All tokens cleared for customer %s", mobile)
    return jsonify({"status": "success", "message": "All tokens cleared"})

# ─────────────────────────────────────────────────────────────────────────────
# OWNER — TEST NOTIFICATION (manual real-time check)
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/owner/test-notification", methods=["POST"])
def test_notification():
    data = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    title = data.get("title", "Test Notification")
    body = data.get("body", "Push notifications are working correctly!")

    if mobile:
        ref = db.collection("owners").document(mobile)
        doc = ref.get()
        if not doc.exists:
            return jsonify({"status": "error", "message": "Owner not found"}), 404
        tokens = _collect_tokens(doc.to_dict())
        if not tokens:
            return jsonify({
                "status": "error",
                "message": "No FCM token found. Please open the app first to register.",
            }), 400
        sent = 0
        for token in tokens:
            try:
                msg = _build_fcm_message(
                    token=token,
                    title=title,
                    body=body,
                    data={
                        "type": "test_notification",
                        "title": title,
                        "body": body,
                    },
                )
                messaging.send(msg)
                sent += 1
            except Exception as e:
                err = str(e).lower()
                logger.warning("[FCM] send failed for token %s...: %s", token[:20], err)
                if (
                    "registration-token-not-registered" in err
                    or "requested entity was not found" in err
                    or "unregistered" in err
                ):
                    ref.update({
                        "fcmTokens": ArrayRemove([token]),
                        "fcmToken": firestore.DELETE_FIELD,
                    })
        return jsonify({"status": "success", "message": "Test notification sent!", "sent_to": sent})

    sent = send_push(title, body, data={"type": "test_notification"})
    return jsonify({"status": "success", "sent_to": sent})


# ═════════════════════════════════════════════════════════════════════════════
# OWNER CONTACT
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/owner/contact", methods=["GET"])
def get_owner_contact():
    for doc in db.collection("owners").stream():
        d = doc.to_dict()
        return jsonify({
            "name":   d.get("name", "Store Owner"),
            "mobile": doc.id,
            "shop":   d.get("shopName", "FreshMart"),
        })
    return jsonify({"name": "Store Owner", "mobile": "", "shop": "FreshMart"})


# ═════════════════════════════════════════════════════════════════════════════
# CATEGORIES
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/categories", methods=["GET"])
def get_categories():
    result = []
    for cat in db.collection("categories").stream():
        d = cat.to_dict()
        d["id"] = cat.id
        d.pop("createdAt", None)
        d.pop("updatedAt", None)
        result.append(d)
    return jsonify(result)


@app.route("/owner/add-category", methods=["POST"])
def add_category():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"status": "error", "message": "name is required"}), 400
    existing_names = [c.to_dict().get("name", "").lower() for c in db.collection("categories").stream()]
    if name.lower() in existing_names:
        return jsonify({"status": "error", "message": f"Category '{name}' already exists"}), 409
    ref = db.collection("categories").document()
    ref.set({"name": name, "imageBase64": data.get("imageBase64") or None,
             "isActive": True, "createdAt": datetime.now(UTC)})
    return jsonify({"status": "success", "message": "Category added", "id": ref.id}), 201


@app.route("/owner/update-category/<category_id>", methods=["PUT"])
def update_category(category_id):
    data = request.json or {}
    db.collection("categories").document(category_id).update({
        "name": data.get("name"), "imageBase64": data.get("imageBase64"),
        "updatedAt": datetime.now(UTC),
    })
    return jsonify({"status": "success", "message": "Category updated successfully"})


@app.route("/owner/delete-category/<category_id>", methods=["DELETE"])
def delete_category(category_id):
    try:
        db.collection("categories").document(category_id).delete()
        return jsonify({"status": "success", "message": "Category deleted"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/owner/toggle-category/<category_id>", methods=["PUT"])
def toggle_category(category_id):
    data = request.json or {}
    ref  = db.collection("categories").document(category_id)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Category not found"}), 404
    ref.update({"isActive": bool(data.get("isActive", True)), "updatedAt": datetime.now(UTC)})
    return jsonify({"status": "success"})


# ═════════════════════════════════════════════════════════════════════════════
# PRODUCTS
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/products", methods=["GET"])
def get_products():
    result = []
    for p in db.collection("products").stream():
        d = p.to_dict()
        d["id"] = p.id
        d.pop("createdAt", None)
        d.pop("updatedAt", None)
        result.append(d)
    return jsonify(result)


@app.route("/owner/add-product", methods=["POST"])
def add_product():
    data = request.json or {}
    # after stock_unit = ...

# inside db.collection("products").add({...}) add:
    name        = (data.get("name") or "").strip()
    category_id = (data.get("categoryId") or "").strip()
    description = (data.get("description") or "").strip()
    price       = data.get("price")
    unit_value  = data.get("unitValue")    # qty per pack  (e.g. 500)
    unit_type   = data.get("unitType")     # unit per pack (e.g. "g")
    quantity    = data.get("quantity")     # stock amount  (e.g. 25)
    stock_unit  = _normalise_unit_str(data.get("stockUnit"))   # stock unit (e.g. "kg") ← NEW
    presets = data.get("presets") or []  
    # Validate required fields
    if not name:
        return jsonify({"status": "error", "message": "name is required"}), 400
    if not category_id:
        return jsonify({"status": "error", "message": "categoryId is required"}), 400
    if price is None:
        return jsonify({"status": "error", "message": "price is required"}), 400
    if unit_value is None or not unit_type:
        return jsonify({"status": "error", "message": "unitValue and unitType are required"}), 400
    if quantity is None:
        return jsonify({"status": "error", "message": "quantity is required"}), 400
    if not stock_unit:
        return jsonify({"status": "error", "message": "stockUnit is required"}), 400

    # Normalise pack unit (kg→g, l→ml) for consistent storage
    canon_value, canon_type = normalise_unit(unit_value, unit_type)

    db.collection("products").add({
        "name":        name,
        "description": description,
        "categoryId":  category_id,
        "imageBase64": data.get("imageBase64") or None,
        "price":       float(price),
        "unitValue":   float(canon_value),   # pack quantity (normalised)
        "unitType":    canon_type,           # pack unit     (normalised)
        "quantity":    float(quantity),  
        "presets": presets,    # stock amount
        "stockUnit":   stock_unit,           # stock unit ← NEW
        "isActive":    True,
        "createdAt":   datetime.now(UTC),
    })
    return jsonify({"status": "success", "message": "Product added successfully"}), 201


@app.route("/owner/update-product/<product_id>", methods=["PUT"])
def update_product(product_id):
    data = request.json or {}
    ref  = db.collection("products").document(product_id)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Product not found"}), 404

    stock_unit = _normalise_unit_str(data.get("stockUnit"))
    if not stock_unit:
        return jsonify({"status": "error", "message": "stockUnit is required"}), 400

    try:
        canon_value, canon_type = normalise_unit(
            float(data.get("unitValue")),
            (data.get("unitType") or "").strip()
        )
        ref.update({
            "name":        (data.get("name") or "").strip(),
            "description": (data.get("description") or "").strip(),
            "categoryId":  (data.get("categoryId") or "").strip(),
            "imageBase64": data.get("imageBase64") or None,
            "price":       float(data.get("price")),
            "unitValue":   float(canon_value),
            "unitType":    canon_type,
            "quantity":    float(data.get("quantity")),
            "stockUnit":   stock_unit,  
            "presets": data.get("presets") or [],            # ← NEW
            "updatedAt":   datetime.now(UTC),
        })
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "price, quantity, unitValue must be numbers"}), 400

    return jsonify({"status": "success", "message": "Product updated"})


@app.route("/owner/delete-product/<product_id>", methods=["DELETE"])
def delete_product(product_id):
    try:
        db.collection("products").document(product_id).delete()
        return jsonify({"status": "success", "message": "Product deleted"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/owner/toggle-product/<product_id>", methods=["PUT"])
def toggle_product(product_id):
    data = request.json or {}
    ref  = db.collection("products").document(product_id)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Product not found"}), 404
    ref.update({"isActive": bool(data.get("isActive", True)), "updatedAt": datetime.now(UTC)})
    return jsonify({"status": "success"})


@app.route("/owner/restock-product/<product_id>", methods=["PUT"])
def restock_product(product_id):
    data = request.json or {}
    ref  = db.collection("products").document(product_id)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Product not found"}), 404
    try:
        qty = float(data.get("quantity"))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "quantity must be a number"}), 400

    update_data = {"quantity": qty, "updatedAt": datetime.now(UTC)}
    # Optionally update stockUnit during restock
    if data.get("stockUnit"):
        update_data["stockUnit"] = _normalise_unit_str(data.get("stockUnit"))

    ref.update(update_data)
    return jsonify({"status": "success", "message": f"Stock updated to {qty}"})


# ═════════════════════════════════════════════════════════════════════════════
# SMART SUGGESTIONS
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/owner/product-suggestions", methods=["GET"])
def product_suggestions():
    query = (request.args.get("q") or "").strip().lower()
    results, seen_keys = [], set()
    for p in db.collection("products").stream():
        d  = p.to_dict()
        nm = (d.get("name") or "").strip()
        if not nm or (query and query not in nm.lower()):
            continue
        key = nm.lower()
        if key in seen_keys:
            continue
        seen_keys.add(key)
        results.append({
            "id":          p.id,
            "name":        nm,
            "description": d.get("description", ""),
            "categoryId":  d.get("categoryId", ""),
            "price":       d.get("price"),
            "unitValue":   d.get("unitValue"),
            "unitType":    d.get("unitType", ""),
            "quantity":    d.get("quantity", 0),
            "stockUnit":   d.get("stockUnit", ""),   # ← included in suggestions
            "imageBase64": d.get("imageBase64") or None,
            "isActive":    d.get("isActive", True),
        })
    results.sort(key=lambda x: x["name"].lower())
    return jsonify(results[:30])


@app.route("/owner/category-suggestions", methods=["GET"])
def category_suggestions():
    query = (request.args.get("q") or "").strip().lower()
    results = []
    for cat in db.collection("categories").stream():
        d    = cat.to_dict()
        name = (d.get("name") or "").strip()
        if not name or (query and query not in name.lower()):
            continue
        results.append({"id": cat.id, "name": name,
                        "imageBase64": d.get("imageBase64") or None,
                        "isActive": d.get("isActive", True)})
    results.sort(key=lambda x: x["name"].lower())
    return jsonify(results)


@app.route("/owner/past-products", methods=["GET"])
def past_products():
    category_filter = (request.args.get("categoryId") or "").strip()
    query           = (request.args.get("q") or "").strip().lower()
    freq = {}
    for order_doc in db.collection("orders").stream():
        for item in order_doc.to_dict().get("items", []):
            pid  = (item.get("productId") or "").strip()
            name = (item.get("name") or "").strip()
            if not pid or not name:
                continue
            if pid not in freq:
                freq[pid] = {"productId": pid, "name": name, "price": item.get("price"),
                             "unitValue": item.get("unitValue"), "unitType": item.get("unitType", ""),
                             "timesOrdered": 0}
            freq[pid]["timesOrdered"] += int(item.get("qty", 1))
    if not freq:
        return jsonify([])
    results = []
    for pid, info in freq.items():
        prod_doc = db.collection("products").document(pid).get()
        if not prod_doc.exists:
            continue
        pd     = prod_doc.to_dict()
        cat_id = pd.get("categoryId", "")
        if category_filter and cat_id != category_filter:
            continue
        if query and query not in info["name"].lower():
            continue
        results.append({
            "id":           pid,
            "name":         info["name"],
            "description":  pd.get("description", ""),
            "categoryId":   cat_id,
            "price":        pd.get("price", info["price"]),
            "unitValue":    pd.get("unitValue", info["unitValue"]),
            "unitType":     pd.get("unitType", info["unitType"]),
            "quantity":     pd.get("quantity", 0),
            "stockUnit":    pd.get("stockUnit", ""),    # ← included
            "imageBase64":  pd.get("imageBase64") or None,
            "timesOrdered": info["timesOrdered"],
            "isActive":     pd.get("isActive", True),
        })
    results.sort(key=lambda x: x["timesOrdered"], reverse=True)
    return jsonify(results[:50])


# ═════════════════════════════════════════════════════════════════════════════
# OWNER DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/owner/dashboard", methods=["GET"])
def owner_dashboard():
    today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    today_orders = today_revenue = pending_orders = total_products = 0
    for order in db.collection("orders").stream():
        data = order.to_dict()
        created_at = data.get("createdAt")
        if created_at and created_at >= today_start:
            today_orders += 1
            today_revenue += data.get("grandTotal", data.get("totalPrice", 0))
        if data.get("status") in ("Pending", "Assigned"):
            pending_orders += 1
    for order in db.collection("delivered_orders").stream():
        data = order.to_dict()
        created_at = data.get("createdAt")
        if created_at and created_at >= today_start:
            today_orders += 1
            today_revenue += data.get("grandTotal", data.get("totalPrice", 0))
    for _ in db.collection("products").stream():
        total_products += 1
    return jsonify({
        "todayOrders":   today_orders,
        "todayRevenue":  today_revenue,
        "pendingOrders": pending_orders,
        "totalProducts": total_products,
    })


# ═════════════════════════════════════════════════════════════════════════════
# OWNER — ORDERS
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/owner/orders", methods=["GET"])
def get_all_orders():
    status_filter = (request.args.get("status") or "").strip()
    include_archived = (request.args.get("includeArchived") or "true").lower() != "false"
    orders = []
    for doc in db.collection("orders").stream():
        d = _order_dict(doc)
        if status_filter and d.get("status") != status_filter:
            continue
        orders.append(d)
    if include_archived:
        for doc in db.collection("delivered_orders").stream():
            d = _order_dict(doc)
            if status_filter and d.get("status") != status_filter:
                continue
            orders.append(d)
    orders.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return jsonify(orders)


@app.route("/owner/order/<order_doc_id>", methods=["GET"])
def get_order_detail(order_doc_id):
    doc = db.collection("orders").document(order_doc_id).get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Order not found"}), 404
    return jsonify(_order_dict(doc))


@app.route("/owner/order/<order_doc_id>/status", methods=["PUT"])
def update_order_status(order_doc_id):
    data       = request.json or {}
    new_status = (data.get("status") or "").strip()
    valid      = {"Pending", "Assigned", "Processing", "Out for Delivery", "Delivered", "Cancelled"}

    if new_status not in valid:
        return jsonify({"status": "error", "message": f"status must be one of: {', '.join(sorted(valid))}"}), 400

    ref = db.collection("orders").document(order_doc_id)
    doc = ref.get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Order not found"}), 404

    order_data = doc.to_dict()
    old_status = order_data.get("status", "")
    customer_mobile = (order_data.get("mobile") or "").strip()
    order_code = order_data.get("orderId") or order_doc_id

    if (new_status == "Cancelled"
            and old_status != "Cancelled"
            and old_status in {"Pending", "Assigned", "Processing", "Out for Delivery"}):
        _restore_stock_for_order(order_data)

    if new_status == "Delivered":
        # ✅ Save a copy to delivered_orders BEFORE deleting
        delivered_data = {**order_data, "status": "Delivered", "deliveredAt": datetime.now(UTC)}
        db.collection("delivered_orders").document(order_doc_id).set(delivered_data)
        if customer_mobile:
            send_customer_push(
                customer_mobile,
                title=f"Order {order_code} delivered",
                body="Your order has been delivered. Thank you!",
                data={"type": "order_status", "status": "Delivered", "orderId": str(order_code)},
            )
        try:
            send_push(
                title=f"Order {order_code} delivered",
                body=f"Order {order_code} has been marked as delivered.",
                data={"type": "order_status", "status": "Delivered", "orderId": str(order_code)},
            )
        except Exception as exc:
            logger.warning("[FCM] Failed to notify owners about delivery %s: %s", order_code, exc)
        ref.delete()
        return jsonify({"status": "success", "message": "Order marked as delivered and removed."})

    ref.update({"status": new_status, "updatedAt": datetime.now(UTC)})
    if customer_mobile:
        send_customer_push(
            customer_mobile,
            title=f"Order {order_code} update",
            body=f"Your order status is now {new_status}.",
            data={"type": "order_status", "status": new_status, "orderId": str(order_code)},
        )
    return jsonify({"status": "success", "message": f"Order status updated to '{new_status}'"})


@app.route("/owner/order/<order_doc_id>", methods=["DELETE"])
def delete_order_by_owner(order_doc_id):
    active_ref = db.collection("orders").document(order_doc_id)
    active_doc = active_ref.get()
    if active_doc.exists:
        order_data = active_doc.to_dict() or {}
        old_status = order_data.get("status", "")
        if old_status in {"Pending", "Assigned", "Processing", "Out for Delivery"}:
            _restore_stock_for_order(order_data)
        active_ref.delete()
        return jsonify({"status": "success", "message": "Order deleted from active orders"})

    archived_ref = db.collection("delivered_orders").document(order_doc_id)
    archived_doc = archived_ref.get()
    if archived_doc.exists:
        archived_ref.delete()
        return jsonify({"status": "success", "message": "Order deleted from archived orders"})

    return jsonify({"status": "error", "message": "Order not found"}), 404

# ═════════════════════════════════════════════════════════════════════════════
# OWNER — USERS
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/owner/users", methods=["GET"])
def get_all_users():
    users = []
    for doc in db.collection("users").stream():
        d = doc.to_dict()
        if d.get("role") != "customer":
            continue
        mobile = doc.id
        order_count = total_spend = 0
        for o in db.collection("orders").where(filter=FieldFilter("mobile", "==", mobile)).stream():
            order_count += 1
            od = o.to_dict()
            total_spend += od.get("grandTotal", od.get("totalPrice", 0))
        for o in db.collection("delivered_orders").where(filter=FieldFilter("mobile", "==", mobile)).stream():
            order_count += 1
            od = o.to_dict()
            total_spend += od.get("grandTotal", od.get("totalPrice", 0))
        created = d.get("createdAt")
        users.append({
            "mobile":     mobile,
            "name":       d.get("name", "Unknown"),
            "role":       d.get("role", "customer"),
            "approved":   d.get("approved", False),
            "orderCount": order_count,
            "totalSpend": total_spend,
            "createdAt":  created.isoformat() if hasattr(created, "isoformat") else None,
        })
    users.sort(key=lambda x: x.get("createdAt") or "", reverse=True)
    return jsonify(users)


@app.route("/owner/users/<mobile>/approval", methods=["PUT"])
def set_user_approval(mobile):
    data     = request.json or {}
    approved = bool(data.get("approved", False))
    user_ref = db.collection("users").document(mobile)
    if not user_ref.get().exists:
        return jsonify({"status": "error", "message": "User not found"}), 404
    user_ref.update({"approved": approved, "updatedAt": datetime.now(UTC)})
    return jsonify({"status": "success", "approved": approved})


# ═════════════════════════════════════════════════════════════════════════════
# CUSTOMER — ORDERS
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/customer/place-order", methods=["POST"])
def place_order():
    data    = request.json or {}
    mobile  = (data.get("mobile") or "").strip()
    items   = data.get("items", [])
    total   = data.get("totalPrice", 0)
    address = (data.get("address") or "").strip()
    lat     = data.get("lat")
    lng     = data.get("lng")

    if not mobile or not items:
        return jsonify({"status": "error", "message": "mobile and items are required"}), 400

    user_ref = db.collection("users").document(mobile)
    user_doc = user_ref.get()
    if not user_doc.exists:
        return jsonify({"status": "error", "code": "NOT_REGISTERED", "message": "Mobile not registered."}), 403

    if not bool(user_doc.to_dict().get("approved", False)):
        return jsonify({
            "status":  "error",
            "code":    "NOT_APPROVED",
            "message": "Your account is not approved yet. Please contact the owner.",
        }), 403

    settings_doc = db.collection("store_settings").document("main").get()
    settings     = settings_doc.to_dict() if settings_doc.exists else {}
    min_order    = float(settings.get("minOrderAmount", 0))
    free_km      = float(settings.get("freeDeliveryKm", 0))
    charge_amt   = float(settings.get("deliveryCharge", 0))

    # ── SERVER-SIDE delivery charge recompute ──
    server_delivery = _compute_delivery_charge(lat, lng, free_km, charge_amt)

    # Recompute correct total using server-side delivery
    items_total   = float(total)   # total is now items-only, no need to subtract
    correct_total = items_total    # keep items total clean; delivery is stored separately

    if min_order > 0 and items_total < min_order:
        return jsonify({
            "status":  "error",
            "code":    "MIN_ORDER",
            "message": f"Minimum order amount is ₹{min_order:.0f}. Add ₹{(min_order - items_total):.0f} more.",
            "minOrderAmount": min_order,
        }), 400

    order_id  = str(uuid.uuid4())[:8].upper()
    order_doc = {
        "orderId":         order_id,
        "mobile":          mobile,
        "items":           items,
        "totalPrice":      round(correct_total, 2),
        "deliveryCharge":  server_delivery,
        "grandTotal":      round(items_total + server_delivery, 2), 
        "address":         address,
        "status":          "Pending",
        "createdAt":       datetime.now(UTC),
    }
    if lat is not None: order_doc["lat"] = float(lat)
    if lng is not None: order_doc["lng"] = float(lng)

    order_ref = db.collection("orders").document()
    order_ref.set(order_doc)

    for item in items:
        pid = (item.get("productId") or "").strip()
        qty = float(item.get("qty", 0))
        if pid and qty > 0:
            prod_ref = db.collection("products").document(pid)
            prod_doc = prod_ref.get()
            if prod_doc.exists:
                prod_data  = prod_doc.to_dict()
                unit_value = float(prod_data.get("unitValue", 0))
                stock_unit = _normalise_unit_str(prod_data.get("stockUnit"))
                cur_stock  = float(prod_data.get("quantity", 0))
                total_base = qty * unit_value
                if stock_unit == "kg":
                    deduct = total_base / 1000
                elif stock_unit == "l":
                    deduct = total_base / 1000
                elif stock_unit in ("g", "ml", "pcs"):
                    deduct = total_base
                else:
                    deduct = total_base if stock_unit == _normalise_unit_str(prod_data.get("unitType")) else qty
                prod_ref.update({"quantity": max(0, cur_stock - deduct)})

    # Fire push notification to owner(s)
    try:
        user_data = user_doc.to_dict() or {}
        customer_name = (data.get("customerName") or user_data.get("name") or mobile).strip()
        item_count = sum(int(i.get("qty", 1)) for i in items)
        item_names = ", ".join((i.get("name") or "") for i in items[:3])
        if len(items) > 3:
            item_names += f" +{len(items)-3} more"

        send_push(
            title=f"New Order #{order_id}",
            body=f"{customer_name} ordered {item_count} item(s): {item_names} - Rs.{items_total:.0f}",
            data={
                "orderId": order_id,
                "docId": order_ref.id,
                "mobile": mobile,
                "totalPrice": str(items_total),
                "type": "new_order",
                "title": f"New Order #{order_id}",
                "body": f"{customer_name} ordered {item_count} item(s)",
            },
        )
    except Exception as exc:
        logger.warning("[FCM] Failed to send new order notification: %s", exc)

    return jsonify({
        "status":  "success",
        "message": "Order placed successfully",
        "orderId": order_id,
        "id":      order_ref.id,
    }), 201
@app.route("/owner/delivery-info", methods=["GET"])
def get_delivery_info():
    settings_doc = db.collection("store_settings").document("main").get()
    settings = settings_doc.to_dict() if settings_doc.exists else {}

    owner_lat = owner_lng = None
    for doc in db.collection("owners").stream():
        d = doc.to_dict()
        owner_lat = d.get("latitude")
        owner_lng = d.get("longitude")
        break

    return jsonify({
        "freeDeliveryKm": settings.get("freeDeliveryKm", 0),
        "deliveryCharge":  settings.get("deliveryCharge", 0),
        "ownerLat":        owner_lat,
        "ownerLng":        owner_lng,
    })
@app.route("/owner/settings", methods=["GET"])
def get_store_settings():
    doc = db.collection("store_settings").document("main").get()
    if not doc.exists:
        return jsonify({"minOrderAmount": 0, "freeDeliveryKm": 0, "deliveryCharge": 0})
    d = doc.to_dict()
    return jsonify({
        "minOrderAmount": d.get("minOrderAmount", 0),
        "freeDeliveryKm": d.get("freeDeliveryKm", 0),
        "deliveryCharge": d.get("deliveryCharge", 0),
    })
@app.route("/customer/orders", methods=["GET"])
def get_customer_orders():
    mobile = (request.args.get("mobile") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile is required"}), 400
    
    orders = []

    # Active orders
    for doc in db.collection("orders").where(filter=FieldFilter("mobile", "==", mobile)).stream():
        d_raw = doc.to_dict()
        if mobile in (d_raw.get("hiddenFor") or []):
            continue
        orders.append(_order_dict(doc))

    # ✅ Delivered orders (archived)
    for doc in db.collection("delivered_orders").where(filter=FieldFilter("mobile", "==", mobile)).stream():
        d_raw = doc.to_dict()
        if mobile in (d_raw.get("hiddenFor") or []):
            continue
        orders.append(_order_dict(doc))

    orders.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return jsonify(orders)

@app.route("/customer/edit-order/<order_doc_id>", methods=["PUT"])
def edit_order_address(order_doc_id):
    data     = request.json or {}
    mobile   = (data.get("mobile") or "").strip()
    address  = (data.get("address") or "").strip()
    lat, lng = data.get("lat"), data.get("lng")
    if not mobile or not address:
        return jsonify({"status": "error", "message": "mobile and address are required"}), 400
    order_ref = db.collection("orders").document(order_doc_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        return jsonify({"status": "error", "message": "Order not found"}), 404
    order_data = order_doc.to_dict()
    if order_data.get("mobile") != mobile:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    if order_data.get("status") not in ("Pending", "Processing"):
        return jsonify({"status": "error", "message": "Cannot edit this order"}), 400
    if lat is not None:
        lat = float(lat)
    if lng is not None:
        lng = float(lng)

    settings_doc = db.collection("store_settings").document("main").get()
    settings = settings_doc.to_dict() if settings_doc.exists else {}
    free_km = float(settings.get("freeDeliveryKm", 0))
    charge_amt = float(settings.get("deliveryCharge", 0))
    server_delivery = _compute_delivery_charge(lat, lng, free_km, charge_amt)
    items_total = float(order_data.get("totalPrice", 0))
    grand_total = round(items_total + server_delivery, 2)

    update = {
        "address": address,
        "deliveryCharge": server_delivery,
        "grandTotal": grand_total,
        "updatedAt": datetime.now(UTC),
    }
    if lat is not None:
        update["lat"] = lat
    if lng is not None:
        update["lng"] = lng
    order_ref.update(update)
    return jsonify({
        "status": "success",
        "message": "Delivery address updated",
        "deliveryCharge": server_delivery,
        "grandTotal": grand_total,
    })


@app.route("/customer/order/<order_doc_id>/items", methods=["PUT"])
def edit_order_items(order_doc_id):
    data      = request.json or {}
    new_items = data.get("items", [])
    new_total = data.get("totalPrice", 0)
    if not new_items:
        return jsonify({"status": "error", "message": "items list cannot be empty"}), 400
    for it in new_items:
        if not it.get("productId") or not it.get("name") or it.get("price") is None or not it.get("qty"):
            return jsonify({"status": "error", "message": "Each item requires: productId, name, price, qty"}), 400
    settings_doc = db.collection("store_settings").document("main").get()
    settings     = settings_doc.to_dict() if settings_doc.exists else {}
    min_order    = float(settings.get("minOrderAmount", 0))
    if min_order > 0 and float(new_total) < min_order:
        return jsonify({
            "status":         "error",
            "code":           "MIN_ORDER",
            "message":        f"Minimum order amount is ₹{min_order:.0f}. Add ₹{(min_order - float(new_total)):.0f} more.",
            "minOrderAmount": min_order,
            "shortfall":      round(min_order - float(new_total), 2),
        }), 400
    order_ref = db.collection("orders").document(order_doc_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        return jsonify({"status": "error", "message": "Order not found"}), 404
    order_data = order_doc.to_dict()
    if order_data.get("status") not in ("Pending", "Processing"):
        return jsonify({"status": "error", "message": "Cannot edit this order"}), 400
    old_qty = {i["productId"]: i.get("qty", 0) for i in order_data.get("items", [])}
    new_qty = {i["productId"]: i.get("qty", 0) for i in new_items}
    for pid, oq in old_qty.items():
        diff = oq - new_qty.get(pid, 0)
        if diff > 0:
            ref = db.collection("products").document(pid)
            d   = ref.get()
            if d.exists: ref.update({"quantity": d.to_dict().get("quantity", 0) + diff})
    for pid, nq in new_qty.items():
        diff = nq - old_qty.get(pid, 0)
        if diff > 0:
            ref = db.collection("products").document(pid)
            d   = ref.get()
            if d.exists:
                cur = d.to_dict().get("quantity", 0)
                if cur < diff:
                    return jsonify({"status": "error", "message": f"Not enough stock for '{pid}'"}), 400
                ref.update({"quantity": cur - diff})
    order_ref.update({"items": new_items, "totalPrice": float(new_total), "updatedAt": datetime.now(UTC)})
    return jsonify({"status": "success", "message": "Order items updated"})

@app.route("/owner/profile", methods=["PUT"])
def update_owner_profile():
    data = request.json or {}

    mobile = (data.get("mobile") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile required"}), 400

    owner_ref = db.collection("owners").document(mobile)
    if not owner_ref.get().exists:
        return jsonify({"status": "error", "message": "Owner not found"}), 404

    update_data = {}

    try:
        name = (data.get("name") or "").strip()
        shop_name = (data.get("shopName") or "").strip()
        if name:
            update_data["name"] = name
        if shop_name:
            update_data["shopName"] = shop_name

        if data.get("latitude") not in (None, ""):
            update_data["latitude"] = float(data.get("latitude"))

        if data.get("longitude") not in (None, ""):
            update_data["longitude"] = float(data.get("longitude"))

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    if not update_data:
        return jsonify({"status": "error", "message": "No valid fields to update"}), 400

    update_data["updatedAt"] = datetime.now(UTC)
    owner_ref.update(update_data)

    return jsonify({"status": "success"})
# This should already be in your app.py — confirm it exists:
@app.route("/owner/profile/<mobile>", methods=["GET"])
def get_owner_profile(mobile):
    owner_ref = db.collection("owners").document(mobile)
    doc = owner_ref.get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Owner not found"}), 404
    d = doc.to_dict()
    return jsonify({
        "mobile":    mobile,
        "name":      d.get("name", ""),
        "shopName":  d.get("shopName", ""),
        "latitude":  d.get("latitude"),
        "longitude": d.get("longitude"),
        "fcmToken":  d.get("fcmToken"),
        "fcmTokens": d.get("fcmTokens", []),
        "tokenUpdatedAt": d.get("tokenUpdatedAt").isoformat() if hasattr(d.get("tokenUpdatedAt"), "isoformat") else d.get("tokenUpdatedAt"),
    })
@app.route("/customer/order/<order_doc_id>/hide", methods=["PUT"])
def hide_order(order_doc_id):
    data   = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile is required"}), 400

    # Check active orders first, then delivered
    order_ref = db.collection("orders").document(order_doc_id)
    order_doc = order_ref.get()

    if not order_doc.exists:
        # ✅ Try delivered_orders
        order_ref = db.collection("delivered_orders").document(order_doc_id)
        order_doc = order_ref.get()

    if not order_doc.exists:
        return jsonify({"status": "error", "message": "Order not found"}), 404
    if order_doc.to_dict().get("mobile") != mobile:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    order_ref.update({"hiddenFor": ArrayUnion([mobile]), "updatedAt": datetime.now(UTC)})
    return jsonify({"status": "success", "message": "Order removed from history"})


# ═════════════════════════════════════════════════════════════════════════════
# SAVED ADDRESSES — CUSTOMER
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/customer/addresses", methods=["GET"])
def list_addresses():
    mobile = (request.args.get("mobile") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile is required"}), 400
    result = []
    for doc in db.collection("customer_addresses").where("mobile", "==", mobile).stream():
        d = doc.to_dict() or {}
        created = d.get("createdAt")
        result.append({
            "id": doc.id, "label": d.get("label", "Home"), "note": d.get("note", ""),
            "address": d.get("address", ""), "lat": d.get("lat"), "lng": d.get("lng"),
            "isDefault": d.get("isDefault", False),
            "createdAt": created.isoformat() if hasattr(created, "isoformat") else None,
        })
    result.sort(key=lambda x: (not x["isDefault"], x["createdAt"] or ""))
    return jsonify(result)


@app.route("/customer/addresses", methods=["POST"])
def add_address():
    data    = request.json or {}
    mobile  = (data.get("mobile") or "").strip()
    label   = (data.get("label") or "Home").strip()
    note    = (data.get("note") or "").strip()
    address = (data.get("address") or "").strip()
    if not mobile: return jsonify({"status": "error", "message": "mobile is required"}), 400
    if not address: return jsonify({"status": "error", "message": "address is required"}), 400
    try:
        lat = float(data.get("lat"))
        lng = float(data.get("lng"))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Valid lat and lng are required"}), 400
    existing   = list(db.collection("customer_addresses").where("mobile", "==", mobile).limit(1).stream())
    is_default = len(existing) == 0
    ref = db.collection("customer_addresses").document()
    ref.set({"mobile": mobile, "label": label, "note": note, "address": address,
             "lat": lat, "lng": lng, "isDefault": is_default, "createdAt": datetime.now(UTC)})
    return jsonify({"status": "success", "id": ref.id, "isDefault": is_default}), 201


@app.route("/customer/addresses/<addr_id>", methods=["PUT"])
def update_address(addr_id):
    data    = request.json or {}
    mobile  = (data.get("mobile") or "").strip()
    address = (data.get("address") or "").strip()
    if not mobile or not address:
        return jsonify({"status": "error", "message": "mobile and address are required"}), 400
    try:
        lat = float(data.get("lat"))
        lng = float(data.get("lng"))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Valid lat and lng are required"}), 400
    doc_ref = db.collection("customer_addresses").document(addr_id)
    doc     = doc_ref.get()
    if not doc.exists or doc.to_dict().get("mobile") != mobile:
        return jsonify({"status": "error", "message": "Address not found"}), 404
    doc_ref.update({"label": (data.get("label") or "Home").strip(), "note": (data.get("note") or "").strip(),
                    "address": address, "lat": lat, "lng": lng, "updatedAt": datetime.now(UTC)})
    return jsonify({"status": "success", "message": "Address updated"})


@app.route("/customer/addresses/<addr_id>", methods=["DELETE"])
def delete_address(addr_id):
    data   = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    doc_ref = db.collection("customer_addresses").document(addr_id)
    doc     = doc_ref.get()
    if not doc.exists or doc.to_dict().get("mobile") != mobile:
        return jsonify({"status": "error", "message": "Address not found"}), 404
    was_default = doc.to_dict().get("isDefault", False)
    doc_ref.delete()
    if was_default:
        remaining = list(db.collection("customer_addresses").where("mobile", "==", mobile).stream())
        if remaining:
            remaining[0].reference.update({"isDefault": True})
    return jsonify({"status": "success", "message": "Address deleted"})


@app.route("/customer/addresses/<addr_id>/default", methods=["PUT"])
def set_default_address(addr_id):
    data   = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile is required"}), 400
    for d in db.collection("customer_addresses").where("mobile", "==", mobile).stream():
        d.reference.update({"isDefault": False})
    doc_ref = db.collection("customer_addresses").document(addr_id)
    doc     = doc_ref.get()
    if not doc.exists or doc.to_dict().get("mobile") != mobile:
        return jsonify({"status": "error", "message": "Address not found"}), 404
    doc_ref.update({"isDefault": True})
    return jsonify({"status": "success", "message": "Default address set"})


# ═════════════════════════════════════════════════════════════════════════════
# CUSTOMER — PROFILE
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/customer/update-profile", methods=["POST"])
def update_profile():
    try:
        data       = request.json or {}
        phone      = (data.get("phone") or "").strip()
        name       = (data.get("name") or "").strip()
        shop_name  = (data.get("shopName") or "").strip()
        shop_image = data.get("shopImage")
        location   = data.get("location") or {}

        if not phone or len(phone) != 10:
            return jsonify({"status": "error", "message": "Invalid phone"}), 400
        if not name:
            return jsonify({"status": "error", "message": "Name required"}), 400
        if location.get("lat") is None or location.get("lng") is None:
            return jsonify({"status": "error", "message": "Location required"}), 400

        user_ref = db.collection("users").document(phone)
        if not user_ref.get().exists:
            return jsonify({"status": "error", "message": "User not found"}), 404

        update_data = {
            "name":     name,
            "shopName": shop_name,
            "location": {
                "lat":     float(location["lat"]),
                "lng":     float(location["lng"]),
                "address": location.get("address", ""),
            },
            "updatedAt": datetime.now(UTC),
        }
        if shop_image:
            try:
                base64.b64decode(shop_image)
                update_data["shopImage"] = shop_image
            except Exception:
                return jsonify({"status": "error", "message": "Invalid image"}), 400

        user_ref.update(update_data)
        return jsonify({"status": "success", "message": "Profile updated"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/customer/profile/<phone>", methods=["GET"])
def get_profile(phone):
    doc = db.collection("users").document(phone).get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "User not found"}), 404
    data = doc.to_dict()
    return jsonify({
        "phone":     data.get("phone"),
        "name":      data.get("name"),
        "shopName":  data.get("shopName"),
        "shopImage": data.get("shopImage"),
        "location":  data.get("location"),
        "fcmToken":  data.get("fcmToken"),
        "fcmTokens": data.get("fcmTokens"),
        "tokenUpdatedAt": data.get("tokenUpdatedAt").isoformat() if hasattr(data.get("tokenUpdatedAt"), "isoformat") else data.get("tokenUpdatedAt"),
    })


# ═════════════════════════════════════════════════════════════════════════════
# BANNERS
# ═════════════════════════════════════════════════════════════════════════════

@app.route("/owner/add-banner", methods=["POST"])
def add_banner():
    try:
        data  = request.json or {}
        image = data.get("imageBase64")
        if not image:
            return jsonify({"status": "error", "message": "Image is required"}), 400
        doc_ref = db.collection("banners").document()
        doc_ref.set({"imageBase64": image, "createdAt": datetime.now(UTC)})
        return jsonify({"status": "success", "id": doc_ref.id}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/owner/banners", methods=["GET"])
def get_banners():
    try:
        result = []
        for doc in db.collection("banners").stream():
            d = doc.to_dict()
            d["id"] = doc.id
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/owner/delete-banner/<id>", methods=["DELETE"])
def delete_banner(id):
    try:
        ref = db.collection("banners").document(id)
        if not ref.get().exists:
            return jsonify({"status": "error", "message": "Banner not found"}), 404
        ref.delete()
        return jsonify({"status": "success", "message": "Deleted"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
# ═══════════════════════════════════════════════════════════════════════════════
#  ADD THESE ROUTES TO app.py
#  Paste them before the  if __name__ == "__main__":  line
# ═══════════════════════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────────────────────
# DELIVERY BOY — AUTH
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/delivery/register", methods=["POST"])
def delivery_register():
    data     = request.json or {}
    mobile   = (data.get("mobile") or "").strip()
    name     = (data.get("name") or "").strip()
    password = (data.get("password") or "").strip()
    vehicle  = (data.get("vehicle") or "").strip()

    if not mobile or len(mobile) != 10 or not mobile.isdigit():
        return jsonify({"status": "error", "message": "Valid 10-digit mobile required"}), 400
    if not name:
        return jsonify({"status": "error", "message": "Name is required"}), 400
    if not password:
        return jsonify({"status": "error", "message": "Password is required"}), 400

    ref = db.collection("delivery_boys").document(mobile)
    if ref.get().exists:
        return jsonify({"status": "error", "message": "Mobile already registered"}), 409

    ref.set({
        "mobile":    mobile,
        "name":      name,
        "vehicle":   vehicle,
        "password":  generate_password_hash(password),
        "isActive":  True,
        "createdAt": datetime.now(UTC),
    })
    return jsonify({
        "status": "success",
        "message": "Registered successfully",
        "boy": {"mobile": mobile, "name": name, "vehicle": vehicle},
    }), 201


@app.route("/delivery/login", methods=["POST"])
def delivery_login():
    data     = request.json or {}
    mobile   = (data.get("mobile") or "").strip()
    password = (data.get("password") or "").strip()

    if not mobile or len(mobile) != 10:
        return jsonify({"status": "error", "message": "Valid mobile required"}), 400

    ref = db.collection("delivery_boys").document(mobile)
    doc = ref.get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Mobile not registered"}), 404

    d = doc.to_dict()
    if not _verify_password(d.get("password", ""), password):
        return jsonify({"status": "error", "message": "Invalid password"}), 401
    if not d.get("isActive", True):
        return jsonify({"status": "error", "message": "Account deactivated. Contact owner."}), 403

    return jsonify({
        "status": "success",
        "boy": {
            "mobile":  mobile,
            "name":    d.get("name", ""),
            "vehicle": d.get("vehicle", ""),
        },
    })


# DELIVERY BOY — FCM TOKEN
@app.route("/delivery/save-fcm-token", methods=["POST"])
def save_delivery_fcm_token():
    data = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    token = (data.get("fcmToken") or "").strip()

    if not mobile or not token:
        return jsonify({"status": "error", "message": "mobile and fcmToken required"}), 400

    ref = db.collection("delivery_boys").document(mobile)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Delivery boy not found"}), 404

    ref.update({
        "fcmToken": token,
        "fcmTokens": ArrayUnion([token]),
        "tokenUpdatedAt": datetime.now(UTC),
    })
    logger.info("[FCM] Token saved for delivery %s: %s...", mobile, token[:20])
    return jsonify({"status": "success", "message": "Token saved"})


@app.route("/delivery/clear-fcm-token", methods=["POST"])
def clear_delivery_fcm_token():
    data = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    token = (data.get("fcmToken") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile required"}), 400

    ref = db.collection("delivery_boys").document(mobile)
    doc = ref.get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Delivery boy not found"}), 404

    update_data = {"tokenUpdatedAt": datetime.now(UTC)}
    if token:
        update_data["fcmTokens"] = ArrayRemove([token])
        if (doc.to_dict() or {}).get("fcmToken") == token:
            update_data["fcmToken"] = firestore.DELETE_FIELD
        ref.update(update_data)
        logger.info("[FCM] Token removed for delivery %s: %s...", mobile, token[:20])
        return jsonify({"status": "success", "message": "Token removed"})

    ref.update({
        "fcmToken": firestore.DELETE_FIELD,
        "fcmTokens": firestore.DELETE_FIELD,
        "tokenUpdatedAt": datetime.now(UTC),
    })
    logger.info("[FCM] All tokens cleared for delivery %s", mobile)
    return jsonify({"status": "success", "message": "All tokens cleared"})


# ─────────────────────────────────────────────────────────────────────────────
# DELIVERY BOY — FETCH ASSIGNED ORDERS  (real-time polling)
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/delivery/orders", methods=["GET"])
def delivery_orders():
    mobile = (request.args.get("mobile") or "").strip()
    if not mobile:
        return jsonify({"status": "error", "message": "mobile required"}), 400

    orders = []

    # Active orders assigned to this delivery boy
    for doc in (
        db.collection("orders")
        .where(filter=FieldFilter("deliveryBoyMobile", "==", mobile))
        .stream()
    ):
        orders.append(_order_dict(doc))

    # Delivered orders (archived collection)
    for doc in (
        db.collection("delivered_orders")
        .where(filter=FieldFilter("deliveryBoyMobile", "==", mobile))
        .stream()
    ):
        orders.append(_order_dict(doc))

    orders.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    return jsonify(orders)


# ─────────────────────────────────────────────────────────────────────────────
# OWNER — LIST ALL DELIVERY BOYS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/owner/delivery-boys", methods=["GET"])
def list_delivery_boys():
    boys = []
    for doc in db.collection("delivery_boys").stream():
        d = doc.to_dict()
        boys.append({
            "mobile":    doc.id,
            "name":      d.get("name", ""),
            "vehicle":   d.get("vehicle", ""),
            "isActive":  d.get("isActive", True),
        })
    boys.sort(key=lambda x: x["name"].lower())
    return jsonify(boys)


# ─────────────────────────────────────────────────────────────────────────────
# OWNER — ASSIGN DELIVERY BOY TO ORDER
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/owner/order/<order_doc_id>/assign-delivery", methods=["PUT"])
def assign_delivery_boy(order_doc_id):
    data            = request.json or {}
    delivery_mobile = (data.get("deliveryBoyMobile") or "").strip()
    delivery_name   = (data.get("deliveryBoyName")   or "").strip()

    ref = db.collection("orders").document(order_doc_id)
    doc = ref.get()
    if not doc.exists:
        return jsonify({"status": "error", "message": "Order not found"}), 404

    update = {
        "deliveryBoyMobile": delivery_mobile,
        "deliveryBoyName":   delivery_name,
        "status":            "Assigned",
        "updatedAt":         datetime.now(UTC),
    }
    # Allow unassigning
    if not delivery_mobile:
        update["deliveryBoyMobile"] = None
        update["deliveryBoyName"]   = None
        update["status"]            = "Pending"

    ref.update(update)
    if delivery_mobile:
        try:
            order_data = doc.to_dict() or {}
            order_id = order_data.get("orderId") or order_doc_id
            customer_mobile = order_data.get("mobile") or ""
            send_delivery_push(
                mobile=delivery_mobile,
                title=f"New Delivery #{order_id}",
                body=f"Order assigned. Pickup & deliver to customer {customer_mobile}.",
                data={
                    "type": "delivery_assignment",
                    "orderId": str(order_id),
                    "docId": order_doc_id,
                    "deliveryBoyMobile": delivery_mobile,
                },
            )
        except Exception as exc:
            logger.warning("[FCM] Failed to notify delivery boy %s: %s", delivery_mobile, exc)
    return jsonify({
        "status":  "success",
        "message": f"Order assigned to {delivery_name}" if delivery_mobile else "Delivery boy removed",
    })


# ─────────────────────────────────────────────────────────────────────────────
# OWNER — UPI INFO  (used by delivery boy app for payment QR)
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/owner/upi", methods=["GET"])
def get_owner_upi():
    for doc in db.collection("owners").stream():
        d = doc.to_dict()
        mobile = str(doc.id or "").strip()
        configured_upi = (d.get("upiId") or "").strip()
        fallback_upi = f"{mobile}@ibl" if mobile else ""
        return jsonify({
            "upiId": configured_upi,
            "fallbackUpiId": fallback_upi,
            "mobile": mobile,
            "name":  d.get("shopName") or d.get("name", "Owner"),
        })
    return jsonify({"upiId": "", "fallbackUpiId": "", "mobile": "", "name": "Owner"})


@app.route("/owner/upi", methods=["PUT"])
def update_owner_upi():
    data   = request.json or {}
    mobile = (data.get("mobile") or "").strip()
    upi_id = (data.get("upiId")  or "").strip()

    if not mobile:
        return jsonify({"status": "error", "message": "mobile required"}), 400

    ref = db.collection("owners").document(mobile)
    if not ref.get().exists:
        return jsonify({"status": "error", "message": "Owner not found"}), 404

    ref.update({"upiId": upi_id, "updatedAt": datetime.now(UTC)})
    return jsonify({"status": "success", "upiId": upi_id})


# ─────────────────────────────────────────────────────────────────────────────
# NOTE: update_order_status already handles "Delivered" → moves to
#       delivered_orders collection. That flow works for delivery boys too
#       since they call the same endpoint:
#       PUT /owner/order/<id>/status   { "status": "Out for Delivery" | "Delivered" }
# ─────────────────────────────────────────────────────────────────────────────

# ═════════════════════════════════════════════════════════════════════════════
# RUN
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug, host="0.0.0.0", port=port)
