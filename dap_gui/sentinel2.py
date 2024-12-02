import json
from shapely.geometry import shape, box


class Sentinel2Tiles:
    def __init__(self, json_file_path):
        """
        Initialize the Sentinel2Tiles object and load data from the JSON file.

        Args:
            json_file_path (str): Path to the JSON file containing tile data.
        """
        self.tiles = {}  # Dictionary to store tile data
        self.load_data(json_file_path)

    def load_data(self, json_file_path):
        """
        Load the JSON data and structure it as a dictionary.

        Args:
            json_file_path (str): Path to the JSON file.
        """
        try:
            with open(json_file_path, "r") as file:
                data = json.load(file)  # Load the JSON data
                # Transform into a dictionary with tile name as the key
                self.tiles = {tile["name"]: tile["geometry"] for tile in data}
        except FileNotFoundError:
            raise Exception(f"File not found: {json_file_path}")
        except json.JSONDecodeError:
            raise Exception(f"Invalid JSON format in file: {json_file_path}")

    def get_geom(self, tilename):
        """
        Retrieve the geometry for a given tile name.

        Args:
            tilename (str): The name of the tile.

        Returns:
            dict: The geometry as a GeoJSON dictionary.

        Raises:
            KeyError: If the tile name does not exist.
        """
        if tilename in self.tiles:
            return json.loads(self.tiles[tilename])
        else:
            raise KeyError(f"Tile '{tilename}' not found in the dataset.")

    def translate2bbox(self, geom):
        """
        Translate a geometry to its bounding box.

        Args:
            geom (dict): A GeoJSON geometry dictionary.

        Returns:
            shapely.geometry.box: The bounding box as a Shapely box.
        """
        geometry = shape(geom)  # Convert GeoJSON to a Shapely geometry
        bounds = geometry.bounds  # Get bounds (minx, miny, maxx, maxy)
        return box(*bounds)

    def enumerate(self):
        """
        Generator that yields tile name and bounding box.

        Yields:
            tuple: (tile name, bounding box)
        """
        for k in self.tiles.keys():
            geom = self.get_geom(k)
            bbox = self.translate2bbox(geom)
            yield k, bbox


if __name__ == "__main__":
    tiles = Sentinel2Tiles("sentinel2_tiles.json")

    # Enumerate through the tiles and print the bounding boxes for the first 10
    for i, (tile_name, bbox) in enumerate(tiles.enumerate()):
        print(f"Tile: {tile_name}, Bounding Box: {bbox}")
        if i == 9:  # Stop after the first 10 tiles
            break
