class RegionDatasetNotFound(Exception):
    pass


class StagingServerUnavailable(Exception):
    """Raised when unable to connect to a staging server.

    This typically occurs when:
    - The PR is old and the staging server has been stopped/removed
    - The staging server hasn't been provisioned yet
    - Network connectivity issues
    """

    pass
