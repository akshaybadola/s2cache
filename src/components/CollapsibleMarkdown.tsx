import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';
import { Accordion, AccordionSummary, AccordionDetails, Typography, Box,
  Button, Collapse} from '@mui/material';
import { ExpandMore, ExpandLess, AutoStories } from '@mui/icons-material';
import 'katex/dist/katex.min.css';

export const CollapsibleMarkdown = ({ content }: { content: string }) => {
  // Splits text at any heading level 2-4
  const sections = content.split(/(?=\n#{2,4}\s|^#{2,4}\s)/g);

  return (
    <Box>
      {sections.map((section, idx) => {
        const trimmed = section.trim();
        if (!trimmed) return null;

        const match = trimmed.match(/^(#{2,4})\s+(.*)/);

        // If it's just a chunk of text or math without a header
        if (!match) {
          return (
            <Box key={idx} sx={{ mb: 2, px: 2 }}>
              <ReactMarkdown remarkPlugins={[remarkMath]} rehypePlugins={[rehypeKatex]}>
                {trimmed}
              </ReactMarkdown>
            </Box>
          );
        }

        const [fullHeader, level, title] = match;
        // Use substring to avoid 'replace' bugs with math symbols
        const body = trimmed.substring(fullHeader.length).trim();

        return (
          <Accordion key={idx} disableGutters sx={{ mb: 1, border: '1px solid #e0e0e0', boxShadow: 'none' }}>
            <AccordionSummary expandIcon={<ExpandMore />}>
              <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                {title}
              </Typography>
            </AccordionSummary>
            <AccordionDetails sx={{ bgcolor: 'action.hover' }}>
              <ReactMarkdown remarkPlugins={[remarkMath]} rehypePlugins={[rehypeKatex]}>
                {body}
              </ReactMarkdown>
            </AccordionDetails>
          </Accordion>
        );
      })}
    </Box>
  );
};

export const CollapsibleSummaryGroup = ({ content }: { content: string }) => {
  const [isMasterExpanded, setIsMasterExpanded] = React.useState(false);

  return (
    <Box sx={{ mt: 1, mb: 4 }}>
      {/* The Master Toggle Button */}
      <Button
        fullWidth
        variant="outlined"
        startIcon={isMasterExpanded ? <ExpandLess /> : <AutoStories />}
        onClick={() => setIsMasterExpanded(!isMasterExpanded)}
        sx={{
          justifyContent: 'flex-start',
          textTransform: 'none',
          py: 1,
          borderRadius: 2,
          borderStyle: isMasterExpanded ? 'solid' : 'dashed',
          color: isMasterExpanded ? 'primary.main' : 'text.secondary',
          borderColor: 'divider',
        }}
      >
        {isMasterExpanded ? 'Hide AI Analysis' : 'View AI Research Analysis'}
      </Button>

      {/* The Container for the Heading Accordions */}
      <Collapse in={isMasterExpanded} timeout="auto">
        <Box sx={{
          mt: 2,
          pl: 2, // Indent to show it belongs to the button above
          borderLeft: '2px solid',
          borderColor: 'primary.light'
        }}>
          <CollapsibleMarkdown content={content} />
        </Box>
      </Collapse>
    </Box>
  );
};
