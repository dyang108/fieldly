import { io, Socket } from 'socket.io-client';
import axios from 'axios';

// Store socket instance
let socket: Socket | null = null;

// Store connected rooms for reference
let connectedRooms: string[] = [];

// Track reconnection attempts
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

export const getSocket = (): Socket => {
  if (!socket) {
    console.log('Creating new socket.io connection');
    
    // Connect to the server
    socket = io('http://localhost:5000', {
      reconnectionAttempts: MAX_RECONNECT_ATTEMPTS,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000,
      timeout: 20000,
      autoConnect: true,
      transports: ['websocket'],  // Only use WebSocket transport with eventlet
      forceNew: false,            // Reuse existing connection if possible
      withCredentials: false,     // Don't send credentials for cross-origin requests
      extraHeaders: {             // Add extra headers for better debugging
        'X-Client-Id': `client-${Math.floor(Math.random() * 1000000)}`
      }
    });
    
    // Add global event listeners
    socket.on('connect', () => {
      console.log('Socket connected with ID:', socket?.id);
      reconnectAttempts = 0; // Reset reconnect attempts on successful connection
      
      // Rejoin previously connected rooms on reconnection
      if (connectedRooms.length > 0) {
        console.log('Rejoining previously connected rooms:', connectedRooms);
        connectedRooms.forEach(room => {
          const [source, datasetName] = room.split('_');
          socket?.emit('join_extraction_room', { 
            source, 
            dataset_name: datasetName 
          });
          console.log(`Rejoined room: ${room}`);
        });
      }
    });
    
    socket.on('disconnect', (reason) => {
      console.log('Socket disconnected, reason:', reason);
      
      // If the server disconnected us, try to reconnect manually
      if (reason === 'io server disconnect') {
        console.log('Server disconnected the socket, attempting to reconnect...');
        socket?.connect();
      }
      
      // If the socket disconnected due to transport close, try to reconnect
      if (reason === 'transport close' && reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        console.log(`Reconnect attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}...`);
        
        setTimeout(() => {
          console.log('Attempting to reconnect after transport close...');
          socket?.connect();
        }, 1000 * reconnectAttempts); // Exponential backoff
      }
    });
    
    socket.on('connect_error', (err) => {
      console.error('Socket connection error:', err);
      reconnectAttempts++;
      
      if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
        console.error('Max reconnection attempts reached, giving up');
      }
    });
    
    socket.on('error', (err) => {
      console.error('Socket error event received:', err);
    });
    
    socket.on('connection_established', (data) => {
      console.log('Connection established with server:', data);
    });
    
    // Handle acknowledgment of joining a room
    socket.on('room_joined', (data) => {
      console.log('Successfully joined room:', data.room);
    });
    
    // Handle acknowledgment of leaving a room
    socket.on('room_left', (data) => {
      console.log('Successfully left room:', data.room);
    });
  }
  
  return socket;
};

export const joinRoom = (source: string, datasetName: string): void => {
  const socket = getSocket();
  const roomId = `${source}_${datasetName}`;
  
  console.log(`Joining room: ${roomId}`);
  
  // Store the room for reconnection
  if (!connectedRooms.includes(roomId)) {
    connectedRooms.push(roomId);
    console.log(`Added ${roomId} to connected rooms list:`, connectedRooms);
  }
  
  // Join the room
  socket.emit('join_extraction_room', { 
    source, 
    dataset_name: datasetName 
  });
};

export const leaveRoom = (source: string, datasetName: string): void => {
  const socket = getSocket();
  const roomId = `${source}_${datasetName}`;
  
  console.log(`Leaving room: ${roomId}`);
  
  // Remove from connected rooms list
  const index = connectedRooms.indexOf(roomId);
  if (index !== -1) {
    connectedRooms.splice(index, 1);
    console.log(`Removed ${roomId} from connected rooms list:`, connectedRooms);
  }
  
  // Leave the room
  socket.emit('leave_extraction_room', {
    source,
    dataset_name: datasetName
  });
};

export const closeSocket = (): void => {
  if (socket) {
    console.log('Closing socket connection');
    // Clear connected rooms list
    connectedRooms.length = 0;
    
    // Disconnect the socket
    socket.disconnect();
    socket = null;
    reconnectAttempts = 0;
  }
};

// Function to check if socket is connected
export const isSocketConnected = (): boolean => {
  return socket?.connected || false;
};

// Function to reconnect socket manually
export const reconnectSocket = (): void => {
  if (socket && !socket.connected) {
    console.log('Manually reconnecting socket...');
    socket.connect();
  }
};

// Function to force create a new socket connection
export const forceNewConnection = (): Socket => {
  if (socket) {
    console.log('Forcing new socket connection...');
    socket.disconnect();
    socket = null;
    reconnectAttempts = 0;
  }
  return getSocket();
};

// Function to check if the server is reachable
export const isServerReachable = async (): Promise<boolean> => {
  try {
    console.log('socket.ts: Checking if server is reachable');
    const response = await axios.get('http://localhost:5000/api/ping', { timeout: 2000 });
    console.log('socket.ts: Server is reachable:', response.status === 200);
    return response.status === 200;
  } catch (err) {
    console.error('socket.ts: Server is not reachable:', err);
    return false;
  }
};

// Function to clear all room data and storage
export const clearAllRoomData = (): void => {
  console.log('socket.ts: Forcing clear of all room data');
  connectedRooms.length = 0;
};

// Add minimal check for existing room
export const isRoomActive = (source: string, datasetName: string): boolean => {
  if (!source || !datasetName) {
    return false;
  }
  
  const roomId = `${source}_${datasetName}`;
  return connectedRooms.includes(roomId);
}; 