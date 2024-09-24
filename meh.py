import os
import logging
import asyncio
import time
from supabase import create_client, Client
import yt_dlp
import cv2  # OpenCV for video processing

# Variables de entorno obtenidas de Render
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SOURCE_VIDEOS_BUCKET = os.getenv('SOURCE_VIDEOS_BUCKET')
CLIPS_BUCKET = os.getenv('CLIPS_BUCKET')

# Crear cliente de Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Función para descargar videos con yt-dlp usando cookies para evitar bloqueos
def download_video(url):
    ydl_opts = {
        'format': 'best',
        'noplaylist': True,
        'quiet': True,
        'cookiefile': 'cookies.txt'  # Usar cookies para evitar problemas de autenticación
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=False)
            logging.info(f"Downloaded video information for {url}")
            return info_dict
    except yt_dlp.utils.DownloadError as e:
        logging.error(f"Failed to download video {url}: {str(e)}")
        return None

# Función para subir a Supabase Storage
def upload_to_supabase_storage(file_path, bucket):
    try:
        with open(file_path, 'rb') as file_data:
            response = supabase.storage.from_(bucket).upload(file_path, file_data)
            logging.info(f"Uploaded {file_path} to Supabase bucket: {bucket}")
            return response
    except Exception as e:
        logging.error(f"Error uploading {file_path} to Supabase: {str(e)}")
        return None

# Función para verificar la subida en Supabase
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
        clip_duration = 60  # Duración de cada clip en segundos (1 minuto en este ejemplo)
        num_clips = int(duration // clip_duration)
        for i in range(num_clips):
            start_time = i * clip_duration
            end_time = start_time + clip_duration
            clip_path = f"clip_{yt_video_id}_{i}.mp4"
            clip_info = {'path': clip_path, 'transcript': f"Transcript of clip {i}"}
            clips.append(clip_info)
            # Aquí iría la lógica para recortar el video en OpenCV o usar FFMPEG
            logging.info(f"Generated clip {clip_path} from {start_time}s to {end_time}s")
        return clips
    except Exception as e:
        logging.error(f"Error processing video {video_path}: {str(e)}")
        return []

# Función para subir los datos de los videos y clips a Supabase
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

# Función principal para procesar los videos
async def process_batch(urls):
    for video_id, url in enumerate(urls):
        logging.info(f"Processing Video ID {video_id}: {url}")
        video_info = download_video(url)
        if video_info:
            yt_video_id = video_info.get('id')
            video_path = f"{yt_video_id}.mp4"  # Asume que se guarda con este nombre
            clips = process_video(video_path, yt_video_id)
            if clips:
                upload_to_supabase(url, video_id, yt_video_id, video_path, video_info, clips)
        time.sleep(10)  # Espera 10 segundos antes de procesar el siguiente video para evitar bloqueos

# URLs de prueba
TEST_URLS = ["https://www.youtube.com/watch?v=EYwLa1ZWD2o"]

# Main function
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(process_batch(TEST_URLS))
