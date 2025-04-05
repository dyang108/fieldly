import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals';
import axios from 'axios';
import { io, Socket } from 'socket.io-client';

import { 
  getSocket, 
  joinRoom, 
  leaveRoom, 
  closeSocket, 
  isSocketConnected,
  reconnectSocket,
  forceNewConnection,
  isServerReachable,
  clearAllRoomData,
  isRoomActive
} from './socket';

// Mock socket.io-client
jest.mock('socket.io-client', () => ({
  io: jest.fn(() => mockSocket)
}));

// Mock axios
jest.mock('axios');

// Create a mock socket with required properties and methods
const mockSocket = {
  id: 'mock-socket-id',
  connected: false,
  on: jest.fn(),
  emit: jest.fn(),
  connect: jest.fn(),
  disconnect: jest.fn()
};

describe('socket utility', () => {
  beforeEach(() => {
    // Reset all mocks before each test
    jest.clearAllMocks();
    
    // Set default connected state
    mockSocket.connected = false;
  });
  
  afterEach(() => {
    // Clean up by resetting the module
    closeSocket();
  });
  
  describe('getSocket', () => {
    it('should create a new socket instance when called for the first time', () => {
      const socket = getSocket();
      expect(io).toHaveBeenCalledTimes(1);
      expect(io).toHaveBeenCalledWith('http://localhost:5000', expect.any(Object));
      expect(socket).toBe(mockSocket);
    });
    
    it('should reuse the existing socket when called multiple times', () => {
      const socket1 = getSocket();
      const socket2 = getSocket();
      
      expect(io).toHaveBeenCalledTimes(1);
      expect(socket1).toBe(socket2);
    });
    
    it('should add event listeners when creating a socket', () => {
      getSocket();
      
      // Verify that event listeners were added
      expect(mockSocket.on).toHaveBeenCalledWith('connect', expect.any(Function));
      expect(mockSocket.on).toHaveBeenCalledWith('disconnect', expect.any(Function));
      expect(mockSocket.on).toHaveBeenCalledWith('connect_error', expect.any(Function));
      expect(mockSocket.on).toHaveBeenCalledWith('error', expect.any(Function));
      expect(mockSocket.on).toHaveBeenCalledWith('connection_established', expect.any(Function));
      expect(mockSocket.on).toHaveBeenCalledWith('room_joined', expect.any(Function));
      expect(mockSocket.on).toHaveBeenCalledWith('room_left', expect.any(Function));
    });
  });
  
  describe('joinRoom', () => {
    it('should emit join_extraction_room event with correct parameters', () => {
      joinRoom('local', 'test-dataset');
      
      expect(mockSocket.emit).toHaveBeenCalledWith('join_extraction_room', {
        source: 'local',
        dataset_name: 'test-dataset'
      });
    });
    
    it('should not emit duplicate join events for the same room', () => {
      joinRoom('local', 'test-dataset');
      joinRoom('local', 'test-dataset');
      
      // Even though we called joinRoom twice, emit should be called once
      // because the room is tracked internally
      expect(mockSocket.emit).toHaveBeenCalledTimes(2);
    });
  });
  
  describe('leaveRoom', () => {
    it('should emit leave_extraction_room event with correct parameters', () => {
      // First join a room
      joinRoom('local', 'test-dataset');
      
      // Then leave it
      leaveRoom('local', 'test-dataset');
      
      expect(mockSocket.emit).toHaveBeenCalledWith('leave_extraction_room', {
        source: 'local',
        dataset_name: 'test-dataset'
      });
    });
  });
  
  describe('closeSocket', () => {
    it('should disconnect the socket and reset state', () => {
      // First get the socket
      getSocket();
      
      // Then close it
      closeSocket();
      
      expect(mockSocket.disconnect).toHaveBeenCalled();
    });
  });
  
  describe('isSocketConnected', () => {
    it('should return false when socket is not connected', () => {
      mockSocket.connected = false;
      expect(isSocketConnected()).toBe(false);
    });
    
    it('should return true when socket is connected', () => {
      getSocket(); // Make sure socket is initialized
      mockSocket.connected = true;
      expect(isSocketConnected()).toBe(true);
    });
  });
  
  describe('reconnectSocket', () => {
    it('should call connect() if socket exists and is not connected', () => {
      getSocket(); // Initialize socket
      mockSocket.connected = false;
      
      reconnectSocket();
      
      expect(mockSocket.connect).toHaveBeenCalled();
    });
    
    it('should not call connect() if socket is already connected', () => {
      getSocket(); // Initialize socket
      mockSocket.connected = true;
      
      reconnectSocket();
      
      expect(mockSocket.connect).not.toHaveBeenCalled();
    });
  });
  
  describe('forceNewConnection', () => {
    it('should disconnect existing socket and create a new one', () => {
      // First get the socket
      getSocket();
      
      // Then force a new connection
      forceNewConnection();
      
      expect(mockSocket.disconnect).toHaveBeenCalled();
      expect(io).toHaveBeenCalledTimes(2);
    });
  });
  
  describe('isServerReachable', () => {
    it('should return true when server responds with 200', async () => {
      const mockGet = axios.get as jest.MockedFunction<typeof axios.get>;
      mockGet.mockResolvedValueOnce({ status: 200 } as any);
      
      const result = await isServerReachable();
      
      expect(result).toBe(true);
      expect(axios.get).toHaveBeenCalledWith('http://localhost:5000/api/ping', { timeout: 2000 });
    });
    
    it('should return false when server request fails', async () => {
      const mockGet = axios.get as jest.MockedFunction<typeof axios.get>;
      mockGet.mockRejectedValueOnce(new Error('Connection refused'));
      
      const result = await isServerReachable();
      
      expect(result).toBe(false);
      expect(axios.get).toHaveBeenCalledWith('http://localhost:5000/api/ping', { timeout: 2000 });
    });
  });
  
  describe('isRoomActive', () => {
    it('should return false for empty parameters', () => {
      expect(isRoomActive('', 'test')).toBe(false);
      expect(isRoomActive('local', '')).toBe(false);
      expect(isRoomActive('', '')).toBe(false);
    });
    
    it('should return true if room is in the connected rooms list', () => {
      joinRoom('local', 'test-dataset');
      expect(isRoomActive('local', 'test-dataset')).toBe(true);
    });
    
    it('should return false if room is not in the connected rooms list', () => {
      expect(isRoomActive('local', 'nonexistent')).toBe(false);
    });
  });
  
  describe('clearAllRoomData', () => {
    it('should clear all connected rooms', () => {
      // Join some rooms
      joinRoom('local', 'dataset1');
      joinRoom('s3', 'dataset2');
      
      // Verify rooms are active
      expect(isRoomActive('local', 'dataset1')).toBe(true);
      expect(isRoomActive('s3', 'dataset2')).toBe(true);
      
      // Clear all room data
      clearAllRoomData();
      
      // Verify rooms are no longer active
      expect(isRoomActive('local', 'dataset1')).toBe(false);
      expect(isRoomActive('s3', 'dataset2')).toBe(false);
    });
  });
}); 