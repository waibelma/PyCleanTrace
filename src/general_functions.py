import os.path
from sys import platform

def build_folders(path_in):
    """Check if an exisiting file path exists. If not create the necessry folder such that the 
    file path existis.

    Args:
    --------
    path_in (str): Project root path

    Returns:
    --------
    Generates the relevant file path
    """

    print("Hi")
    if not os.path.exists(path_in):
        print("Hi")
        if ((platform=="win32") | (platform=="win64")):
            os.mkdir(path_in)
            #os.system("chmod 777 " + path_in)
        elif platform=="darwin":
            os.system("sudo mkdir " + path_in)
            #os.system("sudo chmod 777 " + path_in)
        else:
            raise ValueError('The database folder is not constructed')

def construct_nec_folders(project_path):
    """Check if an exisiting file path exists. If not create the necessry folder such that the 
    file path existis.

    Args:
    --------
    path_in (str): Project root path

    Returns:
    --------
    Generates the relevant file path
    """
    
    ## Define the file paths
    path_input_raw_data_1 = project_path + 'src/original_data/academic_TRACE'
    path_input_raw_data_2 = project_path + 'src/original_data/academic_TRACE/TRACE_raw'
    path_input_raw_data_3 = project_path + 'src/original_data/Mergent_FISD'

    path_output_clean_data_1 = project_path + 'bld/data/TRACE/'
    path_output_clean_data_2 = project_path + 'bld/data/TRACE/TRACE_raw_clean'

    # Construct the relevant folders if they are not already exisiting
    build_folders(path_input_raw_data_1)
    build_folders(path_input_raw_data_2)
    build_folders(path_input_raw_data_3)
    build_folders(path_output_clean_data_1)
    build_folders(path_output_clean_data_2)