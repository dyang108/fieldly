import { describe, it, expect } from '@jest/globals';
import { formatFileName, getFileExtension, isFileType } from './fileUtils';

describe('fileUtils', () => {
  describe('formatFileName', () => {
    it('should return empty string for null or undefined input', () => {
      expect(formatFileName('')).toBe('');
      expect(formatFileName(null as unknown as string)).toBe('');
      expect(formatFileName(undefined as unknown as string)).toBe('');
    });

    it('should return the original file name if it is shorter than maxLength', () => {
      const fileName = 'short-file-name.pdf';
      expect(formatFileName(fileName, 60)).toBe(fileName);
    });

    it('should truncate long file name without paths', () => {
      const longFileName = 'this-is-a-very-long-file-name-that-should-be-truncated-because-it-exceeds-the-limit.pdf';
      const result = formatFileName(longFileName, 30);
      expect(result.length).toBeLessThanOrEqual(30);
      expect(result.startsWith('...')).toBe(true);
      expect(result.endsWith('.pdf')).toBe(true);
    });

    it('should preserve the file name but truncate the path if the path is long', () => {
      const longPath = '/very/long/path/with/multiple/directories/that/should/be/truncated/filename.pdf';
      const result = formatFileName(longPath, 40);
      expect(result.length).toBeLessThanOrEqual(40);
      expect(result.startsWith('...')).toBe(true);
      expect(result.endsWith('filename.pdf')).toBe(true);
    });
  });

  describe('getFileExtension', () => {
    it('should return empty string for null or undefined input', () => {
      expect(getFileExtension('')).toBe('');
      expect(getFileExtension(null as unknown as string)).toBe('');
      expect(getFileExtension(undefined as unknown as string)).toBe('');
    });

    it('should return the file extension without the dot', () => {
      expect(getFileExtension('document.pdf')).toBe('pdf');
      expect(getFileExtension('image.PNG')).toBe('png');
      expect(getFileExtension('data.CSV')).toBe('csv');
    });

    it('should return the last extension for multiple dots', () => {
      expect(getFileExtension('archive.tar.gz')).toBe('gz');
      expect(getFileExtension('config.v1.json')).toBe('json');
    });

    it('should return empty string for files without extension', () => {
      expect(getFileExtension('README')).toBe('');
      expect(getFileExtension('dockerfile')).toBe('');
    });
  });

  describe('isFileType', () => {
    it('should return true if file extension matches any in the allowed list', () => {
      expect(isFileType('document.pdf', ['pdf', 'txt'])).toBe(true);
      expect(isFileType('image.png', ['jpg', 'png', 'gif'])).toBe(true);
      expect(isFileType('data.CSV', ['csv', 'xls'])).toBe(true);
    });

    it('should return false if file extension does not match any in the allowed list', () => {
      expect(isFileType('document.pdf', ['jpg', 'png'])).toBe(false);
      expect(isFileType('README', ['md', 'txt'])).toBe(false);
    });

    it('should handle case insensitivity correctly', () => {
      expect(isFileType('IMAGE.PNG', ['png'])).toBe(true);
      expect(isFileType('document.PDF', ['pdf'])).toBe(true);
    });
  });
}); 