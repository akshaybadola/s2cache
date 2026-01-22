import React, { useState } from 'react';
import {
  Container, Typography, Paper, Box, Collapse,
  ButtonBase, Divider, TextField, Button, CircularProgress,
  Tabs, Tab, Alert
} from '@mui/material';
import { ExpandMore, ExpandLess, Search } from '@mui/icons-material';

import InfiniteList from "../components/InfiniteList";

const BASE_URL = 'http://localhost:9999';

// --- Types based on your Python DataClasses ---
interface PaperDetails {
  paperId: string;
  title: string;
  authors: Array<{ name: string; authorId?: string }>;
  abstract?: string;
  venue?: string;
  year?: string;
  citationCount?: number;
  referenceCount?: number;
  externalIds?: Record<string, string>;
  tldr?: { text: string };
}

interface CitationOrReference {
  citingPaper?: PaperDetails;
  citedPaper?: PaperDetails;
  contexts?: string[];
  intents?: string[];
}

const PaperView: React.FC<{ paper: PaperDetails, onNavigate: (id: string) => void }> = ({ paper, onNavigate }) => {
  const [showMetadata, setShowMetadata] = useState(true);
  const initialCitations = paper.citations?.data || paper.citations || [];
  const initialReferences = paper.references?.data || paper.references || [];

  console.log(paper, initialReferences);

  return (
    <Box>
      {/* Header with toggle for Abstract/Metadata */}
      <ButtonBase
        onClick={() => setShowMetadata(!showMetadata)}
        sx={{
          width: '100%',
          p: 2,
          justifyContent: 'space-between',
          bgcolor: 'action.hover',
          borderRadius: 1,
          mb: 2
        }}
      >
        <Box sx={{ textAlign: 'left' }}>
          <Typography variant="h5" color="primary" sx={{ fontWeight: 'bold' }}>
            {paper.title}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {paper.authors?.map(a => a.name).join(', ')} ({paper.year}) — {paper.venue}
          </Typography>
        </Box>
        {showMetadata ? <ExpandLess /> : <ExpandMore />}
      </ButtonBase>

      <Collapse in={showMetadata}>
        <Box sx={{ p: 2, mb: 2, borderLeft: '4px solid', borderColor: 'primary.main', bgcolor: 'background.paper' }}>
          {/* Metadata Chips (External IDs) */}
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
            {paper.externalIds && Object.entries(paper.externalIds).map(([key, value]) => (
              <Box
                key={key}
                sx={{
                  bgcolor: 'secondary.light',
                  color: 'secondary.contrastText',
                  px: 1.5, py: 0.5,
                  borderRadius: 10,
                  fontSize: '0.7rem',
                  fontWeight: 'bold'
                }}
              >
                {key.toUpperCase()}: {value}
              </Box>
            ))}
          </Box>

          <Typography variant="subtitle2" color="text.secondary" gutterBottom>
            ABSTRACT / TLDR
          </Typography>
          <Typography variant="body1" sx={{ lineHeight: 1.7, color: 'text.primary' }}>
            {paper.tldr?.text || paper.abstract || "No abstract available for this record."}
          </Typography>

          <Box sx={{ mt: 2, display: 'flex', gap: 3 }}>
            <Typography variant="caption"><strong>Citations:</strong> {paper.citationCount ?? 0}</Typography>
            <Typography variant="caption"><strong>References:</strong> {paper.referenceCount ?? 0}</Typography>
          </Box>
        </Box>
      </Collapse>

      <Divider sx={{ my: 4 }} />

      {/* References Section */}
          <Typography variant="h6">References</Typography>
          <Paper variant="outlined">
              <InfiniteList
                  key={`references-${paper.paperId}`} // Crucial fix for tab switching
                  type="references"
                  ssid={paper.paperId}
                  onNavigate={onNavigate}
                  initialItems={paper.references?.data || paper.references || []}
              />
          </Paper>

      {/* Citations Section */}
          <Typography variant="h6">Citations</Typography>
          <Paper variant="outlined" sx={{ mb: 4 }}>
              <InfiniteList
                  key={`citations-${paper.paperId}`} // Crucial fix for tab switching
                  type="citations"
                  ssid={paper.paperId}
                  onNavigate={onNavigate}
                  initialItems={paper.citations?.data || paper.citations || []}
              />
          </Paper>
    </Box>
  );
};

// --- Main App Component ---
const PaperDetailsView: React.FC = () => {
  const [searchInput, setSearchInput] = useState('');
  const [activeTab, setActiveTab] = useState(0);
  const [history, setHistory] = useState<PaperDetails[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchPaper = async (id: string) => {
    setLoading(true);
    setError(null);
    try {
      // Using your /s2_details/<ssid> route
      const response = await fetch(`${BASE_URL}/s2_details/${id}`);
      const data = await response.json();

      if (data.error) throw new Error(data.message || 'Paper not found');

      // Add to history and switch tab
      setHistory(prev => [...prev, data]);
      setActiveTab(history.length);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchInput.trim()) fetchPaper(searchInput.trim());
  };

  return (
    <Container maxWidth="md" sx={{ py: 4 }}>
      <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', textAlign: 'center' }}>
        S2 Paper Explorer
      </Typography>

      {/* Search Bar */}
      <Box component="form" onSubmit={handleSearch} sx={{ display: 'flex', gap: 1, mb: 4 }}>
        <TextField
          fullWidth
          variant="outlined"
          placeholder="Enter ArXiv ID, DOI, or S2 ID (e.g., 1706.03762)"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
        />
        <Button variant="contained" type="submit" startIcon={<Search />} disabled={loading}>
          Search
        </Button>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      {history.length > 0 && (
        <>
          <Tabs value={activeTab} onChange={(_, v) => setActiveTab(v)} variant="scrollable" scrollButtons="auto">
            {history.map((p, i) => (
              <Tab key={i} label={p.title.substring(0, 20) + '...'} />
            ))}
          </Tabs>
          <Box sx={{ mt: 3 }}>
            <PaperView
              paper={history[activeTab]}
              onNavigate={(id) => fetchPaper(id)}
            />
          </Box>
        </>
      )}

      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
          <CircularProgress />
        </Box>
      )}
    </Container>
  );
};


export default PaperDetailsView;
