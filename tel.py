from telethon import TelegramClient
from telethon.errors import FloodWaitError
import asyncio
from datetime import datetime
import pytz
import os

# Telegram API kimlik bilgileri
api_id = ''
api_hash = ''
phone_number = '+'

# TelegramClient başlatma
client = TelegramClient('session_name', api_id, api_hash)

# Dosyaya yazdırma fonksiyonu
def log_to_file(message, file):
    with open(file, 'a', encoding='utf-8') as f:
        f.write(message + "\n")

async def safe_get_messages(client, chat, limit, max_id=None, reply_to_top_id=None):
    while True:
        try:
            messages = await client.get_messages(
                chat, 
                limit=limit, 
                max_id=max_id, 
                reply_to=reply_to_top_id  # Belirtilen konu başlığına ait mesajları çekmek için
            )
            log_to_file(f"Çekilen {len(messages)} mesaj.", log_file)
            return messages
        except FloodWaitError as e:
            log_to_file(f"FloodWaitError: {e.seconds} saniye bekleniyor...", log_file)
            await asyncio.sleep(e.seconds)

async def save_messages_to_file(messages, file_index):
    save_path = r'C:\Users\admin\Desktop' #dosyanın kaydedileceği konum#
    
    # Klasörün var olup olmadığını kontrol et ve yoksa oluştur
    if not os.path.exists(save_path):
        os.makedirs(save_path)
        log_to_file(f"Klasör oluşturuldu: {save_path}", log_file)
    else:
        log_to_file(f"Klasör zaten var: {save_path}", log_file)

    file_path = os.path.join(save_path, f"messages_{file_index}.txt")

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            for message in messages:
                if message.text:  # Mesajda metin var mı diye kontrol et
                    f.write(f"{message.sender_id}: {message.text}\n")
        log_to_file(f"Mesajlar başarıyla kaydedildi: {file_path}", log_file)
    except Exception as e:
        log_to_file(f"Dosyaya yazma hatası: {e}", log_file)

async def main():
    global log_file
    log_file = r'C:\Users\admin\Desktop\log.txt'  # Çıktıların kaydedileceği dosya yolu

    await client.start(phone_number)
    chat_name = ''#telegram grup sohbet linki#
    thread_id = 6969  # Grup altındaki Konu başlığını başlatan mesajın ID'si 

    try:
        chat = await client.get_entity(chat_name)
        
        # Zaman dilimi ekleyerek offset-aware datetime oluşturma
        timezone = pytz.timezone('Europe/Istanbul')
        start_date = timezone.localize(datetime(2024, 11, 1, 0, 0))  # 1 Kasım 2024, 00:00:00
        end_date = timezone.localize(datetime(2024, 11, 2, 0, 0))    # 2 Kasım 2024, 00:00:00

        all_messages = []
        last_id = 0
        total_fetched = 0
        file_index = 1

        while True:
            log_to_file(f"Mesajlar alınıyor... Şu ana kadar çekilen toplam mesaj: {total_fetched}", log_file)
            messages = await safe_get_messages(
                client, 
                chat, 
                limit=500, 
                max_id=last_id, 
                reply_to_top_id=thread_id  # Belirtilen konu başlığına ait mesajları çekmek için
            )

            if not messages:
                log_to_file("Tüm mesajlar alındı veya mesaj çekilemiyor.", log_file)
                break

            for message in messages:
                # Mesajın tarihinin belirtilen aralıkta olup olmadığını kontrol et
                # UTC'den İstanbul'a dönüştürme
                if message.date:  # Tarih kontrolü
                    message_date = message.date.astimezone(timezone)
                    log_to_file(f"Mesaj ID: {message.id}, Tarih: {message_date}, Gönderen ID: {message.sender_id}, Mesaj: {message.text}", log_file)
                
                    # Tarih filtresi burada yapılıyor
                    if start_date <= message_date <= end_date and message.text:
                        all_messages.append(message)

            last_id = messages[-1].id
            total_fetched += len(messages)

            # Eğer 500 mesaj alındıysa, dosyayı kaydet ve listeyi temizle
            if len(all_messages) >= 500:
                await save_messages_to_file(all_messages[:500], file_index)
                all_messages = all_messages[500:]  # İlk 500'ü kaydettikten sonra kalanları tut
                file_index += 1

            await asyncio.sleep(1)  # Rate limit'e takılmamak için bekleme

        # Kalan mesajları kaydet (eğer 500'den az kalmışsa)
        if all_messages:
            await save_messages_to_file(all_messages, file_index)

    except Exception as e:
        log_to_file(f"Bir hata oluştu: {e}", log_file)

# asyncio modülünü kullanarak main fonksiyonunu çalıştırıyoruz
asyncio.run(main())
