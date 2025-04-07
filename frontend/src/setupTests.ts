// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// Learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom';
import { jest } from '@jest/globals';

// Fix for React act warnings
// Set to false to silence the warning about not using act()
// This should only be used if you really can't use act()
// We should wrap renders in act() whenever possible
(global as any).IS_REACT_ACT_ENVIRONMENT = true;

// TextEncoder/TextDecoder polyfill
class TextEncoderPolyfill {
  encode(text: string): Uint8Array {
    const encoded = new Uint8Array(text.length);
    for (let i = 0; i < text.length; i++) {
      encoded[i] = text.charCodeAt(i);
    }
    return encoded;
  }
}

class TextDecoderPolyfill {
  decode(bytes: Uint8Array): string {
    return String.fromCharCode.apply(null, Array.from(bytes));
  }
}

global.TextEncoder = TextEncoderPolyfill as unknown as typeof global.TextEncoder;
global.TextDecoder = TextDecoderPolyfill as unknown as typeof global.TextDecoder;

// Mock the Intersection Observer which is not available in JSDOM
global.IntersectionObserver = class IntersectionObserver {
  constructor(private callback: IntersectionObserverCallback) {}
  observe = jest.fn();
  unobserve = jest.fn();
  disconnect = jest.fn();
  root = null;
  rootMargin = '';
  thresholds = [1];
  takeRecords = jest.fn(() => []);
};

// Mock window.matchMedia
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: jest.fn().mockImplementation((query: string) => {
    return {
      matches: false,
      media: query,
      onchange: null,
      addListener: jest.fn(),
      removeListener: jest.fn(),
      addEventListener: jest.fn(),
      removeEventListener: jest.fn(),
      dispatchEvent: jest.fn(),
    };
  }) as any,
}); 