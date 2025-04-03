import React from 'react';
import { AppBar, Toolbar, Typography, Button, Box } from '@mui/material';
import {
  Dashboard as DashboardIcon,
  Schema as SchemaIcon,
  CloudUpload as UploadIcon
} from '@mui/icons-material';
import { Link, useLocation } from 'react-router-dom';

export default function Navigation() {
  const location = useLocation();
  
  const isActive = (path: string) => {
    return location.pathname === path;
  };
  
  return (
    <AppBar position="static">
      <Toolbar>
        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          Schema Generator
        </Typography>
        
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button
            component={Link}
            to="/"
            color="inherit"
            startIcon={<DashboardIcon />}
            variant={isActive('/') ? 'outlined' : 'text'}
            sx={{ borderColor: 'white' }}
          >
            Datasets
          </Button>
          
          <Button
            component={Link}
            to="/schemas"
            color="inherit"
            startIcon={<SchemaIcon />}
            variant={isActive('/schemas') ? 'outlined' : 'text'}
            sx={{ borderColor: 'white' }}
          >
            Schemas
          </Button>
          
          <Button
            component={Link}
            to="/upload"
            color="inherit"
            startIcon={<UploadIcon />}
            variant={isActive('/upload') ? 'outlined' : 'text'}
            sx={{ borderColor: 'white' }}
          >
            Upload
          </Button>
        </Box>
      </Toolbar>
    </AppBar>
  );
} 