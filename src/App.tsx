import React, { useState } from 'react';
import { AppBar, Toolbar, Tabs, Tab, Box, Container, Typography } from '@mui/material';
import { Search, Science, History } from '@mui/icons-material';

import PaperDetailsView from "./views/PaperDetailsView";
import ResearchDiscoveryView from "./views/ResearchDiscoveryView";
import ResearchStatusView from "./views/ResearchStatusView";


function App() {
  const [activeTabIndex, setActiveTabIndex] = useState(0); // 0=Search, 1=Discovery, 2+=Papers

    const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
        setActiveTabIndex(newValue);
    };

    return (
        <Box sx={{ flexGrow: 1, height: '100vh', display: 'flex', flexDirection: 'column' }}>
            <AppBar position="static" color="default" elevation={1}>
                <Toolbar variant="dense">
                    <Typography variant="h6" sx={{ flexGrow: 0, mr: 4, fontWeight: 'bold', color: 'primary.main' }}>
                        ScholarAI
                    </Typography>

                    <Tabs
                        value={activeTabIndex}
                        onChange={handleTabChange}
                        indicatorColor="primary"
                        textColor="primary"
                    >
                        <Tab icon={<Search fontSize="small" />} iconPosition="start" label="Search" />
                        <Tab icon={<Science fontSize="small" />} iconPosition="start" label="Discovery" />
                        <Tab icon={<History fontSize="small" />} iconPosition="start" label="History" />
                    </Tabs>
                </Toolbar>
            </AppBar>

            <Container maxWidth="xl" sx={{ mt: 3, flexGrow: 1, overflow: 'auto', pb: 4 }}>
                {activeTabIndex === 0 && <PaperDetailsView />}
                {activeTabIndex === 1 && <ResearchDiscoveryView />}
                {activeTabIndex === 2 && <ResearchStatusView stateUrl="/state.json" />}
            </Container>
        </Box>
    );
}


export default App;
