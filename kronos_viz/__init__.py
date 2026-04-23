"""
kronos_viz — 3D geographic visualization for KRONOS-WX cap erosion analysis.

Every layer is anchored to real Oklahoma lat/lon coordinates so spatial
relationships between surface observations, upper-air features, boundaries,
and sounding profiles are immediately readable.

Quick start::

    from kronos_viz import CapErosionScene

    scene = CapErosionScene(title="May 3 1999 — 12Z Cap Analysis")
    scene.add_base_map()
    scene.add_era5_temperature(ds, level_mb=700, valid_time=t)
    scene.add_era5_winds(ds, level_mb=850, valid_time=t)
    scene.add_sounding(profile, lon=-97.44, lat=35.22)
    scene.add_boundary(dryline)
    scene.show()          # opens browser
    scene.save("cap.html")
"""

from .scene import CapErosionScene

__all__ = ["CapErosionScene"]
