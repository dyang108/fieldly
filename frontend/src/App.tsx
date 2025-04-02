import { useState } from 'react'
import { Box, Container, Typography, Paper, Grid, Divider } from '@mui/material'
import FileUpload from './components/FileUpload'
import SchemaManager from './components/SchemaManager'

function App() {
  return (
    <Container maxWidth="xl">
      <Box sx={{ my: 4 }}>
        <Typography variant="h3" component="h1" gutterBottom align="center">
          Schema Generator
        </Typography>
        <Typography variant="subtitle1" gutterBottom align="center" color="text.secondary">
          Upload PDFs and generate schemas using AI
        </Typography>
        
        <Box sx={{ display: 'flex', gap: 4, mt: 2 }}>
          <Paper elevation={3} sx={{ p: 3, flex: 1 }}>
            <Typography variant="h5" component="h2" gutterBottom>
              Upload Files
            </Typography>
            <Divider sx={{ mb: 3 }} />
            <FileUpload />
          </Paper>
          
          <Paper elevation={3} sx={{ p: 3, flex: 1 }}>
            <Typography variant="h5" component="h2" gutterBottom>
              Schema Manager
            </Typography>
            <Divider sx={{ mb: 3 }} />
            <SchemaManager />
          </Paper>
        </Box>
      </Box>
    </Container>
  )
}

export default App
