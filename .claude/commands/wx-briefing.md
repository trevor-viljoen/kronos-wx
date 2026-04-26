# KRONOS-WX Storm Briefing

Run a real-time storm analysis using KRONOS-WX and deliver a structured forecast briefing.

## Steps

1. Run the analysis:
```bash
cd /Users/viljoent/code/kronos-wx && .venv/bin/python main.py analyze-now 2>&1
```

If that fails due to sounding unavailability (Wyoming archive 400/timeout errors), fall back to:
```bash
cd /Users/viljoent/code/kronos-wx && .venv/bin/python main.py analyze-now --forecast-hour 0 2>&1
```

2. Also fetch the current SPC outlook and active watches/warnings for context if not already in the output.

## Output Format

Deliver the briefing as a structured meteorological analysis with these sections:

### KRONOS-WX STORM BRIEFING — {DATE} {TIME}Z

**SITUATION OVERVIEW**
One paragraph plain-English summary of the overall threat picture.

**SPC OUTLOOK**
- Category and tornado probability
- Hatched SIG area (yes/no, where)

**ACTIVE PRODUCTS**
List any tornado watches, severe thunderstorm watches, mesoscale discussions with key bullet points (watch probability, areas, concerns).

**CAP ANALYSIS**
- Current cap strength and MLCIN per available sounding station
- CES erosion timing if available
- Dangerous-capped flag status
- Dryline position/movement if detected

**COUNTY RISK ZONES**
Table or list of counties at EXTREME/HIGH/DANGEROUS_CAPPED tier with their key parameters.

**ENVIRONMENT TENDENCIES**
If tendency data is available: which counties are trending toward initiation (MLCIN decreasing, CAPE/SRH increasing).

**ANALOGUES**
Top historical analogues and what they suggest about storm mode/intensity.

**FORECAST**
Specific timing, locations, and threat level forecast for the next 6 hours. Lead with the most significant threat. Use confidence language (high/moderate/low) based on model and observational agreement. Note any bust scenarios (capping, dry air, outflow).

**DATA NOTES**
Note any data gaps (sounding unavailability, HRRR lag, etc.) and how they affect confidence.
