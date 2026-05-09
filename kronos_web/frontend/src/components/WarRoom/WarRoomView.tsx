import { useState, useMemo, useEffect } from 'react';
import { AnimatePresence } from 'framer-motion';
import { useSSE } from '../../hooks/useSSE';
import { RiskMap } from '../RiskMap';
import { AlertFeed } from '../AlertFeed';
import { MediaWall } from './MediaWall';
import type { DashboardState, Tier } from '../../types/api';

interface WarRoomConfig {
  enabled: boolean;
  error?: string;
  tablo_web_host: string;
  stations: Array<{ call_sign: string; channel_id: string }>;
}

export default function WarRoomView() {
  const state = useSSE<DashboardState>('/api/stream');
  const [config, setConfig] = useState<WarRoomConfig | null>(null);
  const [selectedCounty, setSelectedCounty] = useState<string | null>(null);

  // Fetch War Room configuration
  useEffect(() => {
    fetch('/api/warroom/config')
      .then(r => r.json())
      .then(setConfig)
      .catch(() => setConfig({ enabled: false, tablo_web_host: '', stations: [] }));
  }, []);

  const torActive = (state?.spc?.alerts ?? []).some(a => a.event === 'Tornado Warning');

  if (!state) {
    return (
      <div className="flex h-screen items-center justify-center bg-black">
        <div className="flex flex-col items-center gap-4">
          <div className="w-12 h-12 rounded-full border-2 border-accent border-t-transparent animate-spin" />
          <span className="text-sm font-bold tracking-[0.3em] uppercase text-white/40">Initializing War Room…</span>
        </div>
      </div>
    );
  }

  if (config && (!config.enabled || config.error)) {
    return (
      <div className="flex h-screen items-center justify-center bg-black p-12">
        <div className="max-w-md w-full bg-red-950/20 border border-red-900/50 rounded-xl p-8 backdrop-blur-xl text-center">
          <div className="w-16 h-16 bg-red-900/20 rounded-full flex items-center justify-center mx-auto mb-6">
            <span className="text-2xl">⚠️</span>
          </div>
          <h2 className="text-white font-black tracking-widest uppercase mb-4">Plugin Activation Error</h2>
          <p className="text-white/60 text-sm leading-relaxed mb-8">
            {config.error || "War Room plugin is disabled. Configure credentials in backend to enable."}
          </p>
          <button 
            onClick={() => window.location.href = '/'}
            className="px-6 py-2 bg-white/5 hover:bg-white/10 border border-white/10 rounded-lg text-[10px] font-bold tracking-widest uppercase text-white/40 transition-all"
          >
            Return to Dashboard
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className={`relative h-screen w-screen bg-black overflow-hidden flex flex-col font-sans
                    ${torActive ? 'ring-inset ring-4 ring-red-600/20' : ''}`}>
      
      {/* ── Top Tactical Bar ────────────────────────────────────────────────── */}
      <header className="h-14 flex-shrink-0 bg-black/40 backdrop-blur-xl border-b border-white/5 px-6 flex items-center justify-between z-50">
        <div className="flex items-center gap-6">
          <div className="flex flex-col">
            <span className="text-xs font-black tracking-[0.2em] text-accent italic">KRONOS-WX</span>
            <span className="text-[9px] font-bold text-white/30 uppercase tracking-widest">War Room Mode</span>
          </div>
          
          <div className="h-6 w-px bg-white/10" />
          
          <div className="flex items-center gap-4 font-mono text-xs">
            <div className="flex flex-col">
              <span className="text-white/60">{new Date().getUTCHours().toString().padStart(2, '0')}:{new Date().getUTCMinutes().toString().padStart(2, '0')}Z</span>
              <span className="text-[9px] text-white/20 uppercase tracking-tighter">Current Time</span>
            </div>
            {state.hrrr_valid && (
              <div className="flex flex-col border-l border-white/5 pl-4">
                <span className="text-accent/80">HRRR {state.hrrr_valid}</span>
                <span className="text-[9px] text-white/20 uppercase tracking-tighter">Model Valid</span>
              </div>
            )}
          </div>
        </div>

        {/* NWS Ticker Overlay Placeholder - we can reuse HeaderTicker logic if needed */}
        <div className="flex-1 px-12 overflow-hidden h-full flex items-center">
           {torActive && (
             <div className="flex items-center gap-3 animate-pulse">
               <span className="px-2 py-0.5 bg-red-600 text-[10px] font-black text-white rounded-sm">TOR WARNING</span>
               <span className="text-red-500 font-bold text-xs tracking-tight">LIFE THREATENING WEATHER ACTIVE IN DMA</span>
             </div>
           )}
        </div>

        <div className="flex items-center gap-4">
          <button 
            onClick={() => window.location.href = '/'}
            className="px-4 py-1.5 rounded bg-white/5 hover:bg-white/10 border border-white/10 text-[10px] font-bold tracking-widest uppercase transition-all"
          >
            Exit War Room
          </button>
        </div>
      </header>

      {/* ── Main Tactical Area ──────────────────────────────────────────────── */}
      <main className="flex-1 relative flex">
        
        {/* 1. Tactical Map (Background) */}
        <div className="absolute inset-0 z-0">
          <RiskMap 
            state={state} 
            onCountyClick={setSelectedCounty}
            tactical={true}
          />
        </div>

        {/* 2. Media Wall (Floating Right) */}
        {config && config.enabled && (
          <div className="absolute right-6 top-6 bottom-6 w-[450px] z-20 flex flex-col gap-4">
            <div className="flex-1 bg-black/40 backdrop-blur-2xl border border-white/10 rounded-xl p-4 shadow-2xl flex flex-col overflow-hidden">
               <div className="flex items-center justify-between mb-4 border-b border-white/5 pb-2">
                 <span className="text-[10px] font-black tracking-widest uppercase text-white/40 flex items-center gap-2">
                   <span className="w-2 h-2 rounded-full bg-accent" />
                   Media Command
                 </span>
                 <div className="flex gap-1">
                    <span className="px-1.5 py-0.5 bg-white/5 rounded text-[8px] text-white/30 border border-white/5 font-mono">HOTKEYS [1-3]</span>
                 </div>
               </div>
               
               <MediaWall 
                 tabloHost={config.tablo_web_host}
                 stations={config.stations} 
               />
            </div>

            {/* 3. Tactical Alert Feed (Bottom Right) */}
            <div className="h-1/3 bg-black/40 backdrop-blur-2xl border border-white/10 rounded-xl overflow-hidden shadow-2xl flex flex-col">
              <div className="p-3 border-b border-white/5 bg-black/20">
                <span className="text-[9px] font-black tracking-widest uppercase text-white/40">Tactical Alert Log</span>
              </div>
              <div className="flex-1 overflow-hidden p-2">
                <AlertFeed entries={state.alert_log} />
              </div>
            </div>
          </div>
        )}

        {/* 4. Initiation HUD (Bottom Left) */}
        <div className="absolute left-6 bottom-6 w-72 z-20 pointer-events-none">
           <div className="bg-black/40 backdrop-blur-2xl border border-white/10 rounded-xl p-4 shadow-2xl pointer-events-auto">
              <div className="flex items-center justify-between mb-3 border-b border-white/5 pb-2">
                <span className="text-[10px] font-black tracking-widest uppercase text-amber-500">Initiation Radar</span>
                <span className="text-[9px] text-white/20">CIN Erosion</span>
              </div>
              
              <div className="flex flex-col gap-2">
                {state.initiation_candidates && state.initiation_candidates.length > 0 ? (
                  state.initiation_candidates.slice(0, 5).map(c => {
                    const pt = state.hrrr_counties.find(p => p.county === c);
                    const prob = pt?.cap_break_prob != null ? pt.cap_break_prob : 0;
                    return (
                      <div key={c} className="flex flex-col gap-1">
                        <div className="flex justify-between items-center text-[10px] font-bold">
                          <span className="text-white/80 uppercase">{c.replace(/_/g, ' ')}</span>
                          <span className={prob > 0.7 ? 'text-red-500' : 'text-amber-500'}>{(prob * 100).toFixed(0)}%</span>
                        </div>
                        <div className="h-1 w-full bg-white/5 rounded-full overflow-hidden">
                          <div 
                            className={`h-full transition-all duration-1000 ${prob > 0.7 ? 'bg-red-500' : 'bg-amber-500'}`}
                            style={{ width: `${prob * 100}%` }}
                          />
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="py-4 text-center">
                    <span className="text-[9px] font-bold text-white/10 uppercase tracking-widest italic">No imminent initiation detected</span>
                  </div>
                )}
              </div>
           </div>
        </div>

      </main>

      {/* Global Pulsing Background for Warnings */}
      <AnimatePresence>
        {torActive && (
          <div className="absolute inset-0 pointer-events-none z-10 animate-[pulse_3s_ease-in-out_infinite] bg-red-600/5" />
        )}
      </AnimatePresence>

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 0.3; }
          50% { opacity: 1; }
        }
      `}</style>
    </div>
  );
}
