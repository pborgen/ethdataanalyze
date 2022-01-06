import argparse
import sys

from ethdataanalyze.main import Main

if __name__ == '__main__':

    print('Arguments passed in: ' + str(sys.argv))
   
    parser = argparse.ArgumentParser()
    argparser = argparse.ArgumentParser()
    parser.add_argument("--process", action="store_true", help="Run the full process")
    parser.add_argument('--prependToFileName', action='store', type=str, help="Run the prependToFileName process")
   
    (args, rest) = parser.parse_known_args()

    main = Main();

    if args.process:
        print('About to run the data process engine')
        main.runDataProcessorEngine();
    elif args.prependToFileName:
        print('About to run the data process engine')
        main.prependToFileNamesInDirectory(args.prependToFileName, '.csv');
    else: 
        print('The argument specified does not exist')
        sys.exit()

    # Add the arguments
    # my_parser.add_argument('Run Full data Process engine',
    #                     metavar='data_process_engine',
    #                     type=str,
    #                     help='Run Full data Process engine')

    # my_parser.add_argument('Prepend To File Names',
    #                     metavar='prepend_to_file_name',
    #                     type=str,
    #                     help='prepend the file name to all files in a directory')

    # Execute the parse_args() method
    
    
