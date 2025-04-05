// jest-dom adds custom jest matchers for asserting on DOM nodes.
// Import this file to enable DOM testing utilities
import '@testing-library/jest-dom';
import { expect, jest } from '@jest/globals';

// Add custom matchers to Jest's expect
declare global {
  namespace jest {
    interface Matchers<R> {
      toBeInTheDocument(): R;
      toHaveAttribute(attr: string, value?: string): R;
    }
  }
}

// Mock the TextEncoder/TextDecoder which are not available in JSDOM
if (typeof TextEncoder === 'undefined') {
  class MockTextEncoder {
    encoding = 'utf-8';
    encode(text: string): Uint8Array {
      const arr = new Uint8Array(text.length);
      for (let i = 0; i < text.length; i++) {
        arr[i] = text.charCodeAt(i);
      }
      return arr;
    }
    encodeInto(text: string, dest: Uint8Array): { read: number; written: number } {
      const encoded = this.encode(text);
      const length = Math.min(dest.length, encoded.length);
      dest.set(encoded.subarray(0, length));
      return { read: length, written: length };
    }
  }
  global.TextEncoder = MockTextEncoder as any;
}

if (typeof TextDecoder === 'undefined') {
  class MockTextDecoder {
    encoding = 'utf-8';
    fatal = false;
    ignoreBOM = false;
    
    constructor(_utfLabel = 'utf-8', _options = {}) {
      // Constructor implementation
    }
    
    decode(arr?: Uint8Array): string {
      if (!arr) return '';
      return String.fromCharCode.apply(null, Array.from(arr));
    }
  }
  global.TextDecoder = MockTextDecoder as any;
}

// Mock the Intersection Observer which is not available in JSDOM
class MockIntersectionObserver {
  observe = jest.fn();
  disconnect = jest.fn();
  unobserve = jest.fn();
}

Object.defineProperty(window, 'IntersectionObserver', {
  writable: true,
  configurable: true,
  value: MockIntersectionObserver,
});

// Mock matchMedia
window.matchMedia = window.matchMedia || function() {
  return {
    matches: false,
    media: '',
    onchange: null,
    addListener: function() {},
    removeListener: function() {},
    addEventListener: function() {},
    removeEventListener: function() {},
    dispatchEvent: function() {},
  };
} as any; 