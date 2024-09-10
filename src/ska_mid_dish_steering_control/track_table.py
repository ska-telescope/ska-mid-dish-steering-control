"""Track table for Steering Control Unit (SCU) module."""

import logging
import threading

logger = logging.getLogger("ska-mid-ds-scu")


# pylint: disable=too-many-instance-attributes
class TrackTable:
    """
    Store all the TAI, Azimuth, Elevation points in individual lists.

    A single point can be referred to by the index its data takes in
    the lists, the same in each list.
    """

    def __init__(
        self,
        absolute_times: bool = False,
        additional_offset: float = 0,
        tai_offset: float = 0,
    ) -> None:
        """Initialise TrackTable."""
        self.from_list: bool = True
        self.file_name: str = ""
        self.tai: list[float] = []
        self.azi: list[float] = []
        self.ele: list[float] = []
        self.absolute_times = absolute_times
        self.additional_offset = additional_offset
        self.tai_offset = tai_offset
        self.sent_index: int = 0
        self.num_loaded_batches: int = 0
        self.__points_lock = threading.Lock()
        logger.debug("New TrackTable initialised.")

    def store_from_csv(
        self,
        file_name: str,
    ) -> None:
        """
        Load a track table from a CSV file, storing the points in lists.

        :param str file_name: File name of the track table file including its path.
        :param bool absolute_times: Whether the time column is a real time or a relative
            time. Default False.
        :param float additional_offset: Add additional time to every point. Only has an
            effect when absolute_times is False. Default 5.
        """
        tai = []
        azi = []
        ele = []
        current_line = 1
        try:
            # Load the track table file.
            with open(file_name, "r", encoding="utf-8") as f:
                # Skip the header line because it does not contain a position.
                _ = f.readline()
                current_line += 1
                for line in f:
                    # Remove a trailing '\n' and split the line at every ','.
                    cleaned_line = line.rstrip("\n").split(",")
                    try:
                        tai.append(float(cleaned_line[0]))
                    except IndexError as e:
                        raise IndexError(0) from e

                    try:
                        azi.append(float(cleaned_line[1]))
                    except IndexError as e:
                        raise IndexError(1) from e

                    try:
                        ele.append(float(cleaned_line[2]))
                    except IndexError as e:
                        raise IndexError(2) from e

                    if len(cleaned_line) > 3:
                        raise ValueError(
                            f"Malformed CSV file, line {current_line} is too long."
                        )

                    current_line += 1
        except IndexError as e:
            logger.error(e)
            column = ["Time", "Azimuth", "Elevation"][int(e.args[0])]
            message = f"{column} missing on line {current_line}"
            raise IndexError(message) from e
        except Exception as e:
            logger.error(
                "Could not load or convert the track table file '%s': %s",
                file_name,
                e,
            )
            raise

        self.store_from_list(tai, azi, ele)
        self.from_list = False
        self.file_name = file_name

    def store_from_list(
        self,
        tai: list[float],
        azi: list[float],
        ele: list[float],
    ) -> None:
        """
        Load a track table from input lists file, storing the points in lists.

        :param list[float] tai: A list of times to make up the track table points.
        :param list[float] azi: A list of azimuths to make up the track table points.
        :param list[float] ele: A list of elevations to make up the track table points.
        """
        if not len(tai) == len(azi) == len(ele):
            logger.error(
                "TAI, azimuth, and elevation lists are different lengths, could not"
                "load track table lists."
            )
            return

        with self.__points_lock:
            self.tai.extend(tai)
            self.azi.extend(azi)
            self.ele.extend(ele)
            logger.debug("Stored %s track table points", len(tai))

    def get_next_points(
        self, num_points: int
    ) -> tuple[int, list[float], list[float], list[float]]:
        """
        Get the next num_points of stored points, or the remainder.

        tai_offset is set on the first call, and ignored for subsequent calls.

        :param int num_points: Number of points to get from stored track points.
        :param float tai_offset: The difference in time between now and SKAO epoch in
            seconds.
        :return tuple: The number of points retrieved from the stored points, and a list
            of each point data.
        """
        with self.__points_lock:
            if self.absolute_times:
                tai = self.tai[self.sent_index : self.sent_index + num_points]
            else:
                # Stored times are relative to tai_offset
                tai = [
                    tai_string + self.tai_offset + self.additional_offset
                    for tai_string in self.tai[
                        self.sent_index : self.sent_index + num_points
                    ]
                ]

            azi = self.azi[self.sent_index : self.sent_index + num_points]
            ele = self.ele[self.sent_index : self.sent_index + num_points]
            length = len(tai)
            self.sent_index += length

        return length, tai, azi, ele

    def remaining_points(self) -> int:
        """
        Get the number of points remaining for this track table.

        :return int: The number of points remaining.
        """
        with self.__points_lock:
            points = len(self.tai) - self.sent_index

        return points

    def get_details_string(self) -> str:
        """
        Get a string containing track table object details.

        If the track table object was created from a list, the string contains
        the start and end points of the list this track table object was created from.
        If the track table object was created from a file, the string contains the file
        name.

        :return str: The track table object details.
        """
        if self.from_list:
            with self.__points_lock:
                return (
                    "list starting at time:azimuth:elevation "
                    f"{self.tai[0]}:{self.azi[0]}:{self.ele[0]} "
                    "and ending at time:azimuth:elevation "
                    f"{self.tai[-1]}:"
                    f"{self.azi[-1]}:"
                    f"{self.ele[-1]}"
                )

        return f"file: {self.file_name}"
