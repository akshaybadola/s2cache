import React, { useState, memo } from 'react';
import {
  Typography, Box, IconButton, ListItem,
  Collapse,
} from '@mui/material';
import { ExpandMore, ExpandLess, OpenInNew } from '@mui/icons-material';

const ID_RESOLVERS: Record<string, string> = {
  DOI: 'https://doi.org/',
  ArXiv: 'https://arxiv.org/abs/',
  DBLP: 'https://dblp.org/rec/',
  ACL: 'https://aclanthology.org/',
  PubMed: 'https://pubmed.ncbi.nlm.nih.gov/',
};

const MetadataBadge: React.FC<{ label: string; value: string | number }> = ({ label, value }) => {
  const baseUrl = ID_RESOLVERS[label];

  // If we don't have a resolver, just show the text; otherwise, make it a link
  const isLink = !!baseUrl;

  return (
    <Box
      component={isLink ? "a" : "div"}
      href={isLink ? `${baseUrl}${value}` : undefined}
      target={isLink ? "_blank" : undefined}
      rel={isLink ? "noopener noreferrer" : undefined}
      sx={{
        fontSize: '10px',
        px: 1,
        py: 0.2,
        bgcolor: isLink ? 'blue.50' : 'grey.100',
        color: isLink ? 'primary.main' : 'text.secondary',
        borderRadius: 1,
        textDecoration: 'none',
        display: 'inline-flex',
        alignItems: 'center',
        border: '1px solid',
        borderColor: isLink ? 'blue.100' : 'grey.300',
        '&:hover': isLink ? {
          bgcolor: 'primary.main',
          color: 'white',
          borderColor: 'primary.main'
        } : {}
      }}
    >
      {label}: {value}
    </Box>
  );
};


// --- Optimized Individual Item Component ---
// Memoizing this prevents the "weird lag" because the list only re-renders
// the specific item that changed.
const PaperListItem = memo(({ item, onNavigate }: { item: any, onNavigate: (id: string) => void }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const p = item.citingPaper || item.citedPaper || item;

  if (!p?.title) return null;

  return (
    <ListItem
      divider={!isExpanded}
      sx={{
        flexDirection: 'column',
        alignItems: 'stretch',
        bgcolor: isExpanded ? 'action.hover' : 'transparent',
        transition: 'background-color 0.2s'
      }}
    >
      <Box sx={{ display: 'flex', width: '100%', alignItems: 'center', py: 0.5 }}>
        <Box
          sx={{ flex: 1, cursor: 'pointer' }}
          onClick={() => setIsExpanded(!isExpanded)}
        >
          <Typography variant="body2" sx={{ fontWeight: 600, lineHeight: 1.2 }}>
            {p.title}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            {p.authors?.[0]?.name || 'Unknown'} et al. • {p.year || 'N/A'}
          </Typography>
        </Box>

        <Box>
          <IconButton size="small" onClick={() => onNavigate(p.paperId)} title="Explore Paper">
            <OpenInNew fontSize="inherit" />
          </IconButton>
          <IconButton size="small" onClick={() => setIsExpanded(!isExpanded)}>
            {isExpanded ? <ExpandLess fontSize="inherit" /> : <ExpandMore fontSize="inherit" />}
          </IconButton>
        </Box>
      </Box>

      <Collapse in={isExpanded} timeout="auto" unmountOnExit>
        <Box sx={{
          p: 2, mt: 1, mb: 1,
          bgcolor: 'background.paper',
          borderRadius: 1,
          border: '1px solid',
          borderColor: 'divider',
          boxShadow: 'inset 0px 2px 4px rgba(0,0,0,0.05)'
        }}>
          {p.abstract ? (
            <Typography variant="body2" sx={{ fontStyle: 'italic', mb: 1 }}>
              {p.abstract}
            </Typography>
          ) : (
            <Typography variant="caption" color="text.secondary">No abstract available.</Typography>
          )}
          <Box sx={{ display: 'flex', gap: 1, mt: 1, flexWrap: 'wrap' }}>
            {p.externalIds && Object.entries(p.externalIds).map(([key, val]) => (
              <MetadataBadge key={key} label={key} value={val} />
            ))}
            {/* You can also use it for non-link metadata like Venue */}
            {p.venue && <MetadataBadge label="Venue" value={p.venue} />}
          </Box>
        </Box>
      </Collapse>
    </ListItem>
  );
});

export default PaperListItem;
