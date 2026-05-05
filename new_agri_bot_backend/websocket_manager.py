from typing import List
from fastapi import WebSocket
import json
import logging

logger = logging.getLogger("agri_bot")

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"🔌 New WebSocket connection. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"🔌 WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """
        Отправляет сообщение всем подключенным клиентам.
        """
        if not self.active_connections:
            return

        logger.info(f"📡 Broadcasting message: {message.get('type')}")
        
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"❌ Error sending WS message: {e}")
                disconnected.append(connection)
        
        # Очистка мертвых соединений
        for conn in disconnected:
            self.disconnect(conn)

manager = ConnectionManager()
