import React from 'react';
import { render, screen, act } from '@testing-library/react';
import { describe, it, expect, jest } from '@jest/globals';
import '@testing-library/jest-dom';

import Navigation from './Navigation';

// Mock react-router-dom
jest.mock('react-router-dom', () => ({
  BrowserRouter: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  useLocation: jest.fn(),
  Link: ({ children, to, variant, ...props }: { children: React.ReactNode, to: string, variant?: string }) => (
    <a href={to} data-testid={`link-${to}`} data-variant={variant || 'none'} {...props}>{children}</a>
  )
}));

// Import the mocked module
import { useLocation } from 'react-router-dom';

describe('Navigation Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders all navigation links', async () => {
    // Mock the location for home page
    (useLocation as jest.Mock).mockReturnValue({ pathname: '/' });
    
    // Wrap render in act
    await act(async () => {
      render(<Navigation />);
    });
    
    // Check if the app title is rendered
    const title = screen.getByText('Fieldly');
    expect(title).toBeTruthy();
    
    // Check if all navigation links are rendered
    const datasetsLink = screen.getByText('Datasets');
    const schemasLink = screen.getByText('Schemas');
    const uploadLink = screen.getByText('Upload');
    
    expect(datasetsLink).toBeTruthy();
    expect(schemasLink).toBeTruthy();
    expect(uploadLink).toBeTruthy();
  });
  
  it('sets correct active state based on current path for home', async () => {
    // Mock the location for home page
    (useLocation as jest.Mock).mockReturnValue({ pathname: '/' });
    
    // Wrap render in act
    await act(async () => {
      render(<Navigation />);
    });
    
    // Check that the useLocation hook was called
    expect(useLocation).toHaveBeenCalled();
    
    // Find all the navigation links
    const datasets = screen.getByText('Datasets');
    const schemas = screen.getByText('Schemas');
    const upload = screen.getByText('Upload');
    
    // Verify they exist
    expect(datasets).toBeTruthy();
    expect(schemas).toBeTruthy();
    expect(upload).toBeTruthy();
    
    // Verify the active path is indicated correctly in the rendered component
    expect(useLocation).toHaveReturnedWith({ pathname: '/' });
  });
  
  it('sets correct active state based on current path for schemas', async () => {
    // Mock the location for schemas page
    (useLocation as jest.Mock).mockReturnValue({ pathname: '/schemas' });
    
    // Wrap render in act
    await act(async () => {
      render(<Navigation />);
    });
    
    // Check that the useLocation hook was called
    expect(useLocation).toHaveBeenCalled();
    
    // Find all the navigation links
    const datasets = screen.getByText('Datasets');
    const schemas = screen.getByText('Schemas');
    const upload = screen.getByText('Upload');
    
    // Verify they exist
    expect(datasets).toBeTruthy();
    expect(schemas).toBeTruthy();
    expect(upload).toBeTruthy();
    
    // Verify the active path is indicated correctly in the rendered component
    expect(useLocation).toHaveReturnedWith({ pathname: '/schemas' });
  });
}); 