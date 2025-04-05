/**
 * Utility functions for file handling
 */

/**
 * Format a file name to display by shortening paths if too long
 * 
 * @param fileName The full file name or path
 * @param maxLength The maximum length before truncating (default: 60)
 * @returns The formatted file name
 */
export const formatFileName = (fileName: string, maxLength: number = 60): string => {
  if (!fileName) return '';
  
  // If the file name is not too long, return it as is
  if (fileName.length <= maxLength) {
    return fileName;
  }
  
  // Extract the file name from the path
  const parts = fileName.split('/');
  const name = parts[parts.length - 1];
  
  // If the file name itself is already too long, truncate it
  if (name.length >= maxLength - 3) {
    return `...${name.substring(name.length - (maxLength - 3))}`;
  }
  
  // Otherwise, truncate the path but keep the full file name
  const pathLength = fileName.length - name.length;
  const availableLength = maxLength - name.length - 4; // Account for ".../"
  
  if (availableLength <= 0) {
    return `.../${name}`;
  }
  
  const partialPath = fileName.substring(0, pathLength);
  return `...${partialPath.substring(partialPath.length - availableLength)}/${name}`;
};

/**
 * Extract file extension from file name
 * 
 * @param fileName The file name
 * @returns The file extension (without the dot)
 */
export const getFileExtension = (fileName: string): string => {
  if (!fileName) return '';
  
  const parts = fileName.split('.');
  if (parts.length < 2) return '';
  
  return parts[parts.length - 1].toLowerCase();
};

/**
 * Check if a file is of a specific type based on extension
 * 
 * @param fileName The file name
 * @param extensions Array of allowed extensions
 * @returns True if the file matches any of the extensions
 */
export const isFileType = (fileName: string, extensions: string[]): boolean => {
  const ext = getFileExtension(fileName);
  return extensions.includes(ext);
}; 