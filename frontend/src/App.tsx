import { useState } from 'react'
import { Box, Container, Tab, Tabs, Typography, Paper } from '@mui/material'
import FileUpload from './components/FileUpload'
import SchemaManager from './components/SchemaManager'

interface TabPanelProps {
  children?: React.ReactNode
  index: number
  value: number
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          {children}
        </Box>
      )}
    </div>
  )
}

function App() {
  const [tabValue, setTabValue] = useState(0)

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue)
  }

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Paper elevation={3} sx={{ p: 3 }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <Typography variant="h4" component="h1" gutterBottom align="center">
            Schema Generator
          </Typography>
          
          <Box sx={{ width: '100%', borderBottom: 1, borderColor: 'divider', mb: 3 }}>
            <Tabs 
              value={tabValue} 
              onChange={handleTabChange}
              centered
              sx={{ mb: -1 }}
            >
              <Tab label="File Upload" />
              <Tab label="Schema Manager" />
            </Tabs>
          </Box>

          <Box sx={{ width: '100%' }}>
            <TabPanel value={tabValue} index={0}>
              <FileUpload />
            </TabPanel>
            <TabPanel value={tabValue} index={1}>
              <SchemaManager />
            </TabPanel>
          </Box>
        </Box>
      </Paper>
    </Container>
  )
}

export default App
