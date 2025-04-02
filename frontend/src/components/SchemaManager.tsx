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
  Divider,
  Card,
  CardContent,
  Grid,
  CircularProgress,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Chip
} from '@mui/material'
import { Delete as DeleteIcon, Edit as EditIcon, Add as AddIcon, ExpandMore as ExpandMoreIcon, Send as SendIcon, Refresh as RefreshIcon } from '@mui/icons-material'
import axios from 'axios'
// @ts-ignore
import ReactJson from 'react-json-view'

// Configure axios to use the API endpoint
const api = axios.create({
  baseURL: 'http://localhost:5000'
})

interface Schema {
  id: number
  name: string
  schema: object
  created_at: string
}

type MessageRole = 'user' | 'assistant' | 'system'

interface Message {
  role: MessageRole
  content: string
}

export default function SchemaManager() {
  const [schemas, setSchemas] = useState<Schema[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [notification, setNotification] = useState('')
  
  // Conversation state
  const [userPrompt, setUserPrompt] = useState('')
  const [conversation, setConversation] = useState<Message[]>([])
  const [generatedSchema, setGeneratedSchema] = useState<object | null>(null)
  const [generatingSchema, setGeneratingSchema] = useState(false)
  const [schemaName, setSchemaName] = useState('')

  // Edit state
  const [editingSchema, setEditingSchema] = useState<Schema | null>(null)
  const [editedSchema, setEditedSchema] = useState<object | null>(null)

  useEffect(() => {
    fetchSchemas()
  }, [])

  const fetchSchemas = async () => {
    setLoading(true)
    try {
      const response = await api.get('/api/schemas')
      setSchemas(response.data)
      setError('')
    } catch (err) {
      console.error('Error fetching schemas:', err)
      setError('Failed to fetch schemas')
    } finally {
      setLoading(false)
    }
  }

  const handleSendPrompt = async () => {
    if (!userPrompt.trim()) return
    
    // Add user message to conversation
    const userMessage: Message = { role: 'user', content: userPrompt }
    const updatedConversation = [...conversation, userMessage]
    setConversation(updatedConversation)
    
    // Clear prompt field
    setUserPrompt('')
    
    // Generate schema from conversation
    setGeneratingSchema(true)
    try {
      const response = await api.post('/api/generate-schema', {
        conversation: updatedConversation
      })
      
      // Add assistant response to conversation
      const assistantMessage: Message = { 
        role: 'assistant', 
        content: response.data.message || 'Here is the generated schema.'
      }
      setConversation([...updatedConversation, assistantMessage])
      
      // Set generated schema
      setGeneratedSchema(response.data.schema)
      
      // Default schema name if empty
      if (!schemaName) {
        setSchemaName(response.data.suggested_name || 'New Schema')
      }
      
    } catch (err) {
      console.error('Error generating schema:', err)
      setError('Failed to generate schema')
      // Add error message to conversation
      const errorMessage: Message = {
        role: 'assistant',
        content: 'Sorry, I encountered an error generating the schema.'
      }
      setConversation([...updatedConversation, errorMessage])
    } finally {
      setGeneratingSchema(false)
    }
  }

  const handleSaveGeneratedSchema = async () => {
    if (!generatedSchema) return
    if (!schemaName.trim()) {
      setError('Please provide a name for the schema')
      return
    }

    try {
      await api.post('/api/schemas', {
        name: schemaName,
        schema: generatedSchema
      })
      
      // Reset conversation and generated schema
      setConversation([])
      setGeneratedSchema(null)
      setSchemaName('')
      
      // Show success notification
      setNotification('Schema saved successfully')
      setTimeout(() => setNotification(''), 3000)
      
      // Refresh schemas list
      fetchSchemas()
    } catch (err) {
      console.error('Error saving schema:', err)
      setError('Failed to save schema')
    }
  }

  const handleStartEditing = (schema: Schema) => {
    setEditingSchema(schema)
    setEditedSchema(schema.schema)
  }

  const handleUpdateSchema = async () => {
    if (!editingSchema || !editedSchema) return

    try {
      await api.put(`/api/schemas/${editingSchema.id}`, {
        schema: editedSchema
      })
      
      // Reset editing state
      setEditingSchema(null)
      setEditedSchema(null)
      
      // Show success notification
      setNotification('Schema updated successfully')
      setTimeout(() => setNotification(''), 3000)
      
      // Refresh schemas list
      fetchSchemas()
    } catch (err) {
      console.error('Error updating schema:', err)
      setError('Failed to update schema')
    }
  }

  const handleDeleteSchema = async (id: number) => {
    try {
      await api.delete(`/api/schemas/${id}`)
      
      // Show success notification
      setNotification('Schema deleted successfully')
      setTimeout(() => setNotification(''), 3000)
      
      // Refresh schemas list
      fetchSchemas()
    } catch (err) {
      console.error('Error deleting schema:', err)
      setError('Failed to delete schema')
    }
  }

  const handleResetConversation = () => {
    setConversation([])
    setGeneratedSchema(null)
    setSchemaName('')
  }

  return (
    <Box sx={{ 
      maxWidth: 800, 
      mx: 'auto',
      display: 'flex',
      flexDirection: 'column',
      gap: 2
    }}>
      <Typography variant="h6" align="center" gutterBottom>
        Schema Manager
      </Typography>
      
      {notification && (
        <Alert severity="success" sx={{ mb: 2 }}>
          {notification}
        </Alert>
      )}
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Generate Schema from Conversation
          {conversation.length > 0 && (
            <IconButton size="small" onClick={handleResetConversation} sx={{ ml: 1 }}>
              <RefreshIcon />
            </IconButton>
          )}
        </Typography>
        
        {/* Conversation Display */}
        {conversation.length > 0 && (
          <Box sx={{ mb: 3, maxHeight: 300, overflowY: 'auto', p: 1 }}>
            {conversation.map((message, index) => (
              <Box 
                key={index} 
                sx={{
                  display: 'flex',
                  justifyContent: message.role === 'user' ? 'flex-end' : 'flex-start',
                  mb: 1
                }}
              >
                <Card 
                  sx={{ 
                    maxWidth: '80%',
                    bgcolor: message.role === 'user' ? 'primary.light' : 'grey.100'
                  }}
                >
                  <CardContent sx={{ py: 1, '&:last-child': { pb: 1 } }}>
                    <Typography variant="body2" color={message.role === 'user' ? 'white' : 'text.primary'}>
                      {message.content}
                    </Typography>
                  </CardContent>
                </Card>
              </Box>
            ))}
          </Box>
        )}
        
        {/* Generated Schema Preview */}
        {generatedSchema && (
          <Box sx={{ mb: 3 }}>
            <Typography variant="subtitle1" gutterBottom>
              Generated Schema Preview:
            </Typography>
            <TextField
              fullWidth
              label="Schema Name"
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              margin="normal"
              required
              sx={{ mb: 2 }}
            />
            <Paper sx={{ p: 2, maxHeight: 300, overflowY: 'auto' }}>
              <ReactJson 
                src={generatedSchema} 
                theme="rjv-default" 
                displayDataTypes={false}
                collapsed={1}
              />
            </Paper>
            <Button
              variant="contained"
              color="primary"
              sx={{ mt: 2 }}
              onClick={handleSaveGeneratedSchema}
            >
              Save Schema
            </Button>
          </Box>
        )}
        
        {/* User Input */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <TextField
            fullWidth
            label="Describe your schema in natural language..."
            value={userPrompt}
            onChange={(e) => setUserPrompt(e.target.value)}
            margin="normal"
            multiline
            rows={2}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && e.shiftKey) {
                handleSendPrompt()
                e.preventDefault()
              }
            }}
          />
          <Button
            variant="contained"
            color="primary"
            endIcon={generatingSchema ? <CircularProgress size={20} /> : <SendIcon />}
            onClick={handleSendPrompt}
            disabled={generatingSchema || !userPrompt.trim()}
            sx={{ height: 56, minWidth: 100 }}
          >
            Send
          </Button>
        </Box>
      </Paper>

      <Typography variant="subtitle1" gutterBottom>
        Saved Schemas
      </Typography>
      
      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 4 }}>
          <CircularProgress />
        </Box>
      ) : schemas.length > 0 ? (
        <List>
          {schemas.map((schema) => (
            <Accordion key={schema.id} sx={{ mb: 1 }}>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>{schema.name}</Typography>
                <Chip 
                  size="small" 
                  label={new Date(schema.created_at).toLocaleDateString()} 
                  sx={{ ml: 2 }}
                />
              </AccordionSummary>
              <AccordionDetails>
                <Box sx={{ mb: 2 }}>
                  {editingSchema?.id === schema.id ? (
                    <>
                      <ReactJson 
                        src={editedSchema || {}} 
                        theme="rjv-default" 
                        displayDataTypes={false}
                        onEdit={(edit: any) => setEditedSchema(edit.updated_src)}
                        onAdd={(add: any) => setEditedSchema(add.updated_src)}
                        onDelete={(del: any) => setEditedSchema(del.updated_src)}
                      />
                      <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
                        <Button 
                          variant="contained" 
                          color="primary"
                          onClick={handleUpdateSchema}
                        >
                          Save Changes
                        </Button>
                        <Button 
                          variant="outlined"
                          onClick={() => setEditingSchema(null)}
                        >
                          Cancel
                        </Button>
                      </Box>
                    </>
                  ) : (
                    <>
                      <ReactJson 
                        src={schema.schema} 
                        theme="rjv-default" 
                        displayDataTypes={false}
                        collapsed={1}
                      />
                      <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
                        <Button 
                          variant="outlined" 
                          startIcon={<EditIcon />}
                          onClick={() => handleStartEditing(schema)}
                        >
                          Edit
                        </Button>
                        <Button 
                          variant="outlined" 
                          color="error"
                          startIcon={<DeleteIcon />}
                          onClick={() => handleDeleteSchema(schema.id)}
                        >
                          Delete
                        </Button>
                      </Box>
                    </>
                  )}
                </Box>
              </AccordionDetails>
            </Accordion>
          ))}
        </List>
      ) : (
        <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'center', py: 3 }}>
          No schemas found. Generate one using the conversation interface above.
        </Typography>
      )}
    </Box>
  )
} 