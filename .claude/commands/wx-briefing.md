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

2. The output now includes the full SPC Day 1 narrative. **You must read it before writing any briefing.**

## Critical rules — do not hallucinate

- **KRONOS county risk zones cover only Oklahoma's 77 counties.** They cannot be used to make claims about Kansas, Texas, or any other state.
- **SPC category and tornado probability are based on polygon intersection with the Oklahoma bounding box.** A Kansas MDT that clips the Oklahoma border will show as "ENH" with a SIG hatch in the KRONOS output. This does not mean Oklahoma is the primary threat area.
- **The SPC narrative is the authoritative source for geographic scope.** If the narrative says the main threat is in Kansas, say so. Do not reframe Oklahoma county risk parameters as if they represent the primary severe weather threat.
- **Do not invent watches, MDs, or forecast timing that aren't in the output.** If there are no active products, say so.
- **Do not generate EHI/STP/SRH values beyond what the tool output provides.** Quote or paraphrase the actual numbers.
- **If KRONOS and SPC disagree on the primary threat area, say so explicitly and defer to the SPC narrative.**

## Output Format

Deliver the briefing as a structured meteorological analysis with these sections:

### KRONOS-WX STORM BRIEFING — {DATE} {TIME}Z

**SITUATION OVERVIEW**
One paragraph plain-English summary of the overall threat picture, consistent with the SPC narrative.

**SPC OUTLOOK**
- Quote the SPC summary and primary threat area directly from the narrative
- Category and tornado probability over Oklahoma (note if Oklahoma is peripheral vs. primary)
- Hatched SIG area (yes/no, where — based on narrative, not just the flag)

**ACTIVE PRODUCTS**
List any tornado watches, severe thunderstorm watches, mesoscale discussions with key bullet points. If none, say "No active products."

**CAP ANALYSIS** (Oklahoma-specific)
- Current cap strength and MLCIN per available sounding station
- CES erosion timing if available
- Dangerous-capped flag status
- Dryline position/movement if detected

**COUNTY RISK ZONES** (Oklahoma only)
Table or list of counties at EXTREME/HIGH/DANGEROUS_CAPPED tier with their key parameters. Note if the SPC narrative does not corroborate these as primary threat areas.

**ENVIRONMENT TENDENCIES**
If tendency data is available: which counties are trending toward initiation (MLCIN decreasing, CAPE/SRH increasing).

**ANALOGUES**
Top historical analogues and what they suggest about storm mode/intensity.

**FORECAST**
Specific timing, locations, and threat level — consistent with the SPC narrative. If the primary threat is outside Oklahoma, say so. Lead with what the SPC says is most likely. Use confidence language (high/moderate/low). Note any bust scenarios.

**DATA NOTES**
Note any data gaps (sounding unavailability, HRRR lag, etc.) and how they affect confidence.
