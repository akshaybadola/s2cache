import React, { Fragment, useState } from 'react';
import {
  Typography, Paper, Box, Collapse,
  ButtonBase, Divider
} from "@mui/material";
import { ExpandMore, ExpandLess } from '@mui/icons-material';

import { PaperDetails } from "../Models";
import InfiniteList from "./InfiniteList";


const PaperCard: React.FC<{ paper: PaperDetails, onNavigate: (id: string) => void }> =
  ({ paper, onNavigate, showReferences, showCitations, showAbstract }) => {
  const [showMetadata, setShowMetadata] = useState(false);
  const initialCitations = paper.citations?.data || paper.citations || [];
  const initialReferences = paper.references?.data || paper.references || [];

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

          {showAbstract ?
            <>
             <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                 ABSTRACT / TLDR
             </Typography>
             <Typography variant="body1" sx={{ lineHeight: 1.7, color: 'text.primary' }}>
                 {paper.tldr?.text || paper.abstract || "No abstract available for this record."}
             </Typography>
            </> : null}
          <Box sx={{ mt: 2, display: 'flex', gap: 3 }}>
            <Typography variant="caption"><strong>Citations:</strong> {paper.citationCount ?? 0}</Typography>
            <Typography variant="caption"><strong>References:</strong> {paper.referenceCount ?? 0}</Typography>
          </Box>
        </Box>
      </Collapse>

      <Divider sx={{ my: 4 }} />

      {/* References Section */}
      {showReferences ?
        <>
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
        </> : null}

      {/* Citations Section */}
      {showCitations ?
          <>
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
          </> : null}
    </Box>
  );
};

export default PaperCard;
