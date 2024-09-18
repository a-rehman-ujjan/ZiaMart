from twilio.rest import Client
from Notifications.modules import Notification
from Notifications import settings

account_sid = settings.TWILIO_ACCOUNT_SID
auth_token = settings.TWILIO_AUTH_TOKEN
phone = settings.PHONE
client = Client(account_sid, auth_token)

async def Order_Notification(message: Notification):
    message = client.messages.create(
    from_='whatsapp:+14155238886',
    body=message,
    to=(f'whatsapp:+{phone}')
    )
    print(message.sid)