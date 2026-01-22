export interface PaperDetails {
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

export interface CitationOrReference {
  citingPaper?: PaperDetails;
  citedPaper?: PaperDetails;
  contexts?: string[];
  intents?: string[];
}
