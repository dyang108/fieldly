import { useState, useEffect } from 'react'
import {
  Box,
  Button,
  TextField,
  Typography,
  Paper,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material'
import { Delete as DeleteIcon, Edit as EditIcon } from '@mui/icons-material'
import axios from 'axios'

interface Schema {
  id: number
  name: string
  content: any
  created_at: string
  updated_at: string
}

export default function SchemaManager() {
  const [schemas, setSchemas] = useState<Schema[]>([])
  const [openDialog, setOpenDialog] = useState(false)
  const [editingSchema, setEditingSchema] = useState<Schema | null>(null)
  const [name, setName] = useState('')
  const [content, setContent] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    fetchSchemas()
  }, [])

  const fetchSchemas = async () => {
    try {
      const response = await axios.get('/api/schemas')
      setSchemas(response.data)
    } catch (err) {
      setError('Failed to fetch schemas')
    }
  }

  const handleOpenDialog = (schema?: Schema) => {
    if (schema) {
      setEditingSchema(schema)
      setName(schema.name)
      setContent(JSON.stringify(schema.content, null, 2))
    } else {
      setEditingSchema(null)
      setName('')
      setContent('')
    }
    setOpenDialog(true)
  }

  const handleCloseDialog = () => {
    setOpenDialog(false)
    setEditingSchema(null)
    setName('')
    setContent('')
    setError('')
  }

  const handleSave = async () => {
    try {
      const schemaData = {
        name,
        content: JSON.parse(content)
      }

      if (editingSchema) {
        await axios.put(`/api/schemas/${editingSchema.id}`, schemaData)
      } else {
        await axios.post('/api/schemas', schemaData)
      }

      handleCloseDialog()
      fetchSchemas()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save schema')
    }
  }

  const handleDelete = async (id: number) => {
    try {
      await axios.delete(`/api/schemas/${id}`)
      fetchSchemas()
    } catch (err) {
      setError('Failed to delete schema')
    }
  }

  return (
    <Box sx={{ maxWidth: 800, mx: 'auto' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h6">Schema Manager</Typography>
        <Button variant="contained" onClick={() => handleOpenDialog()}>
          New Schema
        </Button>
      </Box>

      {error && (
        <Typography color="error" sx={{ mb: 2 }}>
          {error}
        </Typography>
      )}

      <Paper>
        <List>
          {schemas.map((schema) => (
            <ListItem key={schema.id}>
              <ListItemText
                primary={schema.name}
                secondary={`Created: ${new Date(schema.created_at).toLocaleString()}`}
              />
              <ListItemSecondaryAction>
                <IconButton
                  edge="end"
                  aria-label="edit"
                  onClick={() => handleOpenDialog(schema)}
                  sx={{ mr: 1 }}
                >
                  <EditIcon />
                </IconButton>
                <IconButton
                  edge="end"
                  aria-label="delete"
                  onClick={() => handleDelete(schema.id)}
                >
                  <DeleteIcon />
                </IconButton>
              </ListItemSecondaryAction>
            </ListItem>
          ))}
        </List>
      </Paper>

      <Dialog open={openDialog} onClose={handleCloseDialog} maxWidth="md" fullWidth>
        <DialogTitle>
          {editingSchema ? 'Edit Schema' : 'New Schema'}
        </DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            margin="normal"
            required
          />
          <TextField
            fullWidth
            label="Content (JSON)"
            value={content}
            onChange={(e) => setContent(e.target.value)}
            margin="normal"
            required
            multiline
            rows={10}
            error={!!error}
            helperText={error}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog}>Cancel</Button>
          <Button onClick={handleSave} variant="contained">
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
} 