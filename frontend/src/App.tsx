import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Container, CssBaseline, Box, Paper, Typography } from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import SchemaManager from './components/SchemaManager';
import FileUpload from './components/FileUpload';
import DatasetGrid from './components/DatasetGrid';
import DatasetDetailView from './components/DatasetDetailView';
import Navigation from './components/Navigation';

// Create theme
const theme = createTheme({
  palette: {
    primary: {
      main: '#3f51b5',
    },
    secondary: {
      main: '#f50057',
    },
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
        <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
          <Navigation />
          
          <Container maxWidth="lg" sx={{ mt: 4, mb: 4, flexGrow: 1 }}>
            <Routes>
              <Route path="/" element={<DatasetGrid />} />
              <Route path="/schemas" element={<SchemaManager />} />
              <Route path="/upload" element={<FileUpload />} />
              <Route path="/dataset/:source/:datasetName" element={<DatasetDetailView />} />
            </Routes>
          </Container>
          
          <Paper 
            sx={{ 
              marginTop: 'auto', 
              py: 2, 
              textAlign: 'center' 
            }} 
            component="footer" 
            square 
            variant="outlined"
          >
            <Typography variant="body2" color="text.secondary">
              Schema Generator © {new Date().getFullYear()}
            </Typography>
          </Paper>
        </Box>
      </Router>
    </ThemeProvider>
  );
}

export default App;
