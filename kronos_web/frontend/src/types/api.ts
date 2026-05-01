// TypeScript interfaces matching the DashboardState JSON from api.py

export type Tier =
  | 'EXTREME'
  | 'HIGH'
  | 'DANGEROUS_CAPPED'
  | 'MODERATE'
  | 'MARGINAL'
  | 'LOW'

export type SPCCategory = 'HIGH' | 'MDT' | 'ENH' | 'SLGT' | 'MRGL' | 'TSTM' | 'NONE'

export interface RiskZone {
  tier: Tier
  tier_rank: number
  counties: string[]
  center_lat: number
  center_lon: number
  lat_min: number
  lat_max: number
  lon_min: number
  lon_max: number
  peak_MLCAPE: number
  peak_MLCIN: number
  peak_SRH_0_1km: number
  peak_EHI: number
}

export interface CountyPoint {
  county: string
  lat: number
  lon: number
  MLCAPE: number
  MLCIN: number
  SBCAPE: number
  SBCIN: number
  SRH_0_1km: number
  SRH_0_3km: number
  BWD_0_6km: number
  lapse_rate: number | null
  dewpoint_2m_F: number
  LCL_height_m: number | null
  EHI: number | null
  STP: number | null
  cap_break_prob: number | null
  convergence_score: number | null
  in_watch: boolean
  alarm_bell: boolean
}

export interface SoundingData {
  station: string
  valid_time: string | null
  MLCAPE: number
  MLCIN: number
  SBCAPE: number
  SBCIN: number
  MUCAPE: number
  LCL_height: number
  LFC_height: number | null
  cap_strength: number
  convective_temperature: number
  lapse_rate_700_500: number
  precipitable_water: number
  SRH_0_1km: number
  SRH_0_3km: number
  BWD_0_6km: number
  EHI: number | null
  STP: number | null
  SCP: number | null
  LLJ_speed: number | null
}

export interface EnvironmentData {
  oun: SoundingData
  lmn: SoundingData | null
  fwd: SoundingData | null
  fetched_hour: number
}

export interface CESData {
  cap_behavior: string
  erosion_hour: number | null
  tc_gap_12Z: number | null
  tc_gap_18Z: number | null
}

export interface MoistureData {
  state_mean_dewpoint_f: number
  moisture_return_gradient_f: number
  gulf_moisture_fraction: number
  n_stations: number
}

export interface DrylineData {
  position_lat: number[]
  position_lon: number[]
  confidence: number
  surge_mph: number | null
  counties: string[]
  motion_speed: number | null
}

export interface SPCOutlookData {
  category: SPCCategory
  max_tornado_prob: number | null
  sig_tornado_hatched: boolean
  issued_utc: string | null
}

export interface AlertData {
  event: string
  headline: string
  area_desc: string
  expires_utc: string | null
  expires_label: string
  description: string
  watch_number: number | null
  priority: number
}

export interface MDData {
  number: number
  url: string
  areas_affected: string
  concerning: string
  body_lines: string[]
  prob_watch: number | null
}

export interface SPCData {
  outlook: SPCOutlookData | null
  alerts: AlertData[]
  mds: MDData[]
}

export interface TendencyRow {
  county: string
  tier: Tier
  d_cin: number
  d_cape: number
  d_srh1: number
  d_srh3: number
  d_ehi: number
  trend: string
  trend_level: 'improving2' | 'improving' | 'steady' | 'degrading'
}

export interface ModelForecast {
  sig_pct: number
  count_exp: number
  count_lo: number
  count_hi: number
}

export interface AlertLogEntry {
  ts: string
  msg: string
}

export interface AnalogueEntry {
  case_id: string
  date: string
  event_class: string | null
  tornado_count: number
  cap_behavior: string | null
  distance: number
  MLCAPE: number | null
  MLCIN: number | null
  cap_strength: number | null
  SRH_0_1km: number | null
  EHI: number | null
  tc_gap_12Z: number | null
  spc_url: string
}

export interface StationObs {
  station_id: string
  county: string
  lat: number
  lon: number
  temp_f: number
  dewpoint_f: number
  wind_dir: number
  wind_speed: number
  wind_gust: number | null
}

export type BoundaryType = 'DRYLINE' | 'OUTFLOW' | 'FRONTAL' | 'DIFFERENTIAL_HEATING' | 'OLD_MCS_REMNANT'

export interface BoundaryData {
  boundary_type: BoundaryType
  detected_by: string
  position_lat: number[]
  position_lon: number[]
  confidence: number
  motion_speed: number | null
  motion_direction: number | null
  counties: string[]
  valid_time: string | null
}

export interface BoundaryInteractionData {
  interaction_point_lat: number
  interaction_point_lon: number
  interaction_county: string
  alarm_bell_flag: boolean
  convergence_magnitude: number | null
  boundary_1_type: string
  boundary_2_type: string
  notes: string | null
}

export type BriefSeverity = 'critical' | 'elevated' | 'favorable' | 'neutral'

export interface BriefLine {
  label: string
  value: string
  detail: string
  severity: BriefSeverity
}

export interface DashboardState {
  updated_at: string | null
  hrrr_valid: string | null
  risk_zones: RiskZone[]
  hrrr_counties: CountyPoint[]
  tier_map: Record<string, Tier>
  environment: EnvironmentData | null
  ces: CESData | null
  moisture: MoistureData | null
  dryline: DrylineData | null
  boundaries: BoundaryData[]
  boundary_interactions: BoundaryInteractionData[]
  tendency: TendencyRow[]
  spc: SPCData
  alert_geojson: GeoJSON.FeatureCollection | null
  outlook_geojson: GeoJSON.FeatureCollection | null
  torn_geojson: GeoJSON.FeatureCollection | null
  wind_geojson: GeoJSON.FeatureCollection | null
  hail_geojson: GeoJSON.FeatureCollection | null
  mesonet_obs: StationObs[]
  model_forecast: ModelForecast | null
  alert_log: AlertLogEntry[]
  analogues: AnalogueEntry[]
  initiation_candidates: string[]
  situation_brief: BriefLine[]
}
