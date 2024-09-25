import os
import logging
import asyncio
from supabase import create_client
import yt_dlp
import cv2  # OpenCV para el procesamiento de videos
import time  # Para limitar las solicitudes si es necesario

# Variables de entorno obtenidas de Render
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SOURCE_VIDEOS_BUCKET = os.getenv('SOURCE_VIDEOS_BUCKET')
CLIPS_BUCKET = os.getenv('CLIPS_BUCKET')

# Crear cliente de Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Función para descargar videos con yt-dlp, usando cookies para evitar el bloqueo
def download_video(url):
    ydl_opts = {
        'format': 'best',
        'noplaylist': True,
        'quiet': True,
        'cookiefile': 'cookies.txt'  # Ruta al archivo de cookies
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=False)
            video_title = info_dict.get('title', None)
            ydl.download([url])
            return info_dict
    except Exception as e:
        logging.error(f"Failed to download video {url}: {str(e)}")
        return None

# Función para subir archivos a Supabase Storage
def upload_to_supabase_storage(file_path, bucket):
    try:
        with open(file_path, 'rb') as file_data:
            response = supabase.storage.from_(bucket).upload(file_path, file_data)
            logging.info(f"Uploaded {file_path} to Supabase bucket: {bucket}")
            return response
    except Exception as e:
        logging.error(f"Error uploading {file_path} to Supabase: {str(e)}")
        return None

# Verificación de subida a Supabase
def verify_supabase_upload(file_path, bucket):
    try:
        file_info = supabase.storage.from_(bucket).get_public_url(file_path)
        return file_info is not None
    except Exception as e:
        logging.error(f"Error verifying upload: {str(e)}")
        return False

# Procesamiento de video para generar clips
def process_video(video_path, yt_video_id):
    clips = []
    try:
        cap = cv2.VideoCapture(video_path)
        length = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        duration = length / fps
        clip_duration = 60  # Duración de cada clip (60 segundos en este ejemplo)
        num_clips = int(duration // clip_duration)
        for i in range(num_clips):
            start_time = i * clip_duration
            end_time = start_time + clip_duration
            clip_path = f"clip_{yt_video_id}_{i}.mp4"
            clip_info = {'path': clip_path, 'transcript': f"Transcript of clip {i}"}
            clips.append(clip_info)
            logging.info(f"Generated clip {clip_path} from {start_time}s to {end_time}s")
        return clips
    except Exception as e:
        logging.error(f"Error processing video {video_path}: {str(e)}")
        return []

# Subida de videos y clips a Supabase
def upload_to_supabase(url, video_id, yt_video_id, video_path, video_info, clips):
    try:
        # Subir video original
        video_storage_path = upload_to_supabase_storage(video_path, SOURCE_VIDEOS_BUCKET)
        if not verify_supabase_upload(video_storage_path, SOURCE_VIDEOS_BUCKET):
            logging.error(f"Failed to upload video {video_path}")
            return

        # Guardar información del video en la base de datos
        video_data = {
            "id": video_id,
            "filename": os.path.basename(video_storage_path),
            "storage_path": video_storage_path,
            "url": url,
            "title": video_info.get('title', ''),
            "description": video_info.get('description', ''),
            "channel": video_info.get('uploader', ''),
            "published_at": video_info.get('upload_date', '')
        }
        supabase.table('videos').upsert(video_data).execute()

        # Subir clips y guardar en la base de datos
        for clip in clips:
            clip_storage_path = upload_to_supabase_storage(clip['path'], CLIPS_BUCKET)
            if verify_supabase_upload(clip_storage_path, CLIPS_BUCKET):
                clip_data = {
                    "video_id": video_id,
                    "file_path": clip_storage_path,
                    "original_youtube_url": url,
                    "transcript": clip['transcript']
                }
                supabase.table('clips').insert(clip_data).execute()
            else:
                logging.error(f"Failed to upload clip {clip['path']}")
    except Exception as e:
        logging.error(f"Error uploading data to Supabase: {str(e)}")

# Procesar los videos en lote
async def process_batch(urls):
    for video_id, url in enumerate(urls):
        logging.info(f"Processing Video ID {video_id}: {url}")
        video_info = download_video(url)
        if video_info:
            yt_video_id = video_info.get('id')
            video_path = f"{yt_video_id}.mp4"
            clips = process_video(video_path, yt_video_id)
            if clips:
                upload_to_supabase(url, video_id, yt_video_id, video_path, video_info, clips)
        else:
            logging.error(f"Video download failed for {url}")

# URLs de prueba
TEST_URLS = ["https://www.youtube.com/watch?v=sample_video"]

# Función principal
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(process_batch(TEST_URLS))
