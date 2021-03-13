
import os
from unittest.mock import patch

import pytest
from tqdm import tqdm

import omnidiff
from omnidiff.command_line import main, plural
from omnidiff.dirs import FileInfo
from omnidiff.file import FileStats

def info_options(**kwargs):
    # Default options (in alphabetical order since that's what Mock wants)
    args = dict(
        fast=False,
        no_empty=False,
        progress=tqdm,
        resume=False,
        threads=1,
    )
    args.update(kwargs)
    return args

def dupes_options(**kwargs):
    # Default options (in alphabetical order since that's what Mock wants)
    # The defaults are different from the info command.
    args = dict(
        fast=True,
        no_empty=False,
        progress=tqdm,
        resume=True,
        threads=1,
    )
    args.update(kwargs)
    return args

@pytest.mark.parametrize('count,singular,multiple,expected', [
    [0, 'foo', None, '0 foos'],
    [1, 'foo', None, '1 foo'],
    [2, 'foo', None, '2 foos'],
    [0, 'foo', 'foos', '0 foos'],
    [1, 'foo', 'foos', '1 foo'],
    [2, 'foo', 'foos', '2 foos'],
    [0, 'box', 'boxes', '0 boxes'],
    [1, 'box', 'boxes', '1 box'],
    [2, 'box', 'boxes', '2 boxes'],
])
def test_plural(count, singular, multiple, expected):
    assert(plural(count, singular, multiple) == expected)

# I'm not too interested in testing that every possible set of command-line
# options "really work", since it most cases they're the same as the function
# options. So, we just test that the options are accepted and converted as
# expected.
expected_commands = ['info', 'dupes']
expected_options = ['--version', '--help']

def check_help_message(capsys):
    output = capsys.readouterr()
    assert output.out.startswith('Usage: omnidiff')
    commands_list = output.out.rpartition('Commands:')[2].split()
    for command in expected_commands:
        assert command in commands_list, f'Command {command} missing from help'
    options_list = [
        word for word in output.out.rpartition('Options:')[2].split()
        if word.startswith('--')
    ]
    for option in expected_options:
        assert option in options_list, f'Option {option} missing from help'

@patch('sys.argv', ['omnidiff', '--help'])
def test_main_help(capsys):
    """
    Help message includes at least usage message and top-level options.
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    check_help_message(capsys)

@patch('sys.argv', ['omnidiff'])
def test_main(capsys):
    """
    Running with no arguments displays the help message.
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    check_help_message(capsys)

@patch('sys.argv', ['omnidiff', '--version'])
def test_main_version(capsys):
    """
    --version option shows the version of the package.
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    output = capsys.readouterr()
    assert output.out == f'omnidiff, version {omnidiff.__version__}\n'

@patch('sys.argv', ['omnidiff', 'info'])
def test_info_missing_param(capsys):
    """
    info command takes one compulsory argument, the dirname.
    """
    with pytest.raises(SystemExit, match='2'):
        main()
    output = capsys.readouterr()
    assert "Missing argument 'DIRNAME'" in output.err

@patch('sys.argv', ['omnidiff', 'info', '.'])
@patch('omnidiff.command_line.DirInfo')
def test_info(mock_DirInfo, capsys):
    """
    info command populates and saves the specified directory.
    """
    absname = os.path.abspath('.')
    mock_DirInfo.return_value.save.return_value = 'FILENAME'
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.assert_called_once_with(absname)
    mock_DirInfo.return_value.populate.assert_called_once_with(
        **info_options()
    )
    mock_DirInfo.return_value.save.assert_called_once()
    assert capsys.readouterr().out.startswith('Written FILENAME\n')

@patch('sys.argv', ['omnidiff', 'info', '.', '--no-populate'])
@patch('omnidiff.command_line.DirInfo')
def test_info_no_populate(mock_DirInfo):
    """
    info command doesn't populate if you tell it not to.
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.cached.assert_called_once_with(os.path.abspath('.'))
    mock_DirInfo.cached.return_value.populate.assert_not_called()

@patch('sys.argv', ['omnidiff', 'info', '.', '--no-hash-empty'])
@patch('omnidiff.command_line.DirInfo')
def test_info_no_empty(mock_DirInfo):
    """
    Command-line supports populate() options: no_empty
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.return_value.populate.assert_called_once_with(
        **info_options(no_empty=True)
    )

@patch('sys.argv', ['omnidiff', 'info', '.', '--fast'])
@patch('omnidiff.command_line.DirInfo')
def test_info_fast(mock_DirInfo):
    """
    Command-line supports populate() options: fast
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.return_value.populate.assert_called_once_with(
        **info_options(fast=True)
    )

@patch('sys.argv', ['omnidiff', 'info', '.', '--resume'])
@patch('omnidiff.command_line.DirInfo')
def test_info_resume(mock_DirInfo):
    """
    Command-line supports populate() options: resume
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.return_value.populate.assert_not_called()
    mock_DirInfo.cached.return_value.populate.assert_called_once_with(
        **info_options(resume=True)
    )

@patch('sys.argv', ['omnidiff', 'info', '.', '--threads=99'])
@patch('omnidiff.command_line.DirInfo')
def test_info_threads(mock_DirInfo):
    """
    Command-line supports populate() options: threads
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.return_value.populate.assert_called_once_with(
        **info_options(threads=99)
    )

@patch('sys.argv', ['omnidiff', 'dupes'])
def test_dupes_missing_param(capsys):
    """
    info command takes one compulsory argument, the dirname.
    """
    with pytest.raises(SystemExit, match='2'):
        main()
    output = capsys.readouterr()
    assert "Missing argument 'DIRNAME'" in output.err

@patch('sys.argv', ['omnidiff', 'dupes', '.'])
@patch('omnidiff.command_line.DirInfo')
def test_dupes(mock_DirInfo):
    """
    dupes command reports on dupe groups, populating only if necessary.
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.cached.assert_called_once_with(os.path.abspath('.'))
    mock_DirInfo.cached.return_value.populate.assert_not_called()
    mock_DirInfo.cached.return_value.dupe_groups.assert_called()

@patch('sys.argv', ['omnidiff', 'dupes', '.'])
@patch('omnidiff.command_line.DirInfo')
def test_dupes_output(mock_DirInfo, capsys):
    """
    dupes command lists the members of the dupe groups.
    """
    mock_DirInfo.cached.return_value.dupe_groups.return_value = [
        frozenset({
            FileInfo.add_hash(FileStats('.', 'a', 123), b'111'),
            FileInfo.add_hash(FileStats('.', 'b', 123), b'111'),
        })
    ]
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.cached.return_value.dupe_groups.assert_called()
    expected = '2 duplicates with size 123, hash 313131\n  a\n  b\n'
    assert capsys.readouterr().out == expected

@patch('sys.argv', ['omnidiff', 'dupes', '.'])
@patch('omnidiff.command_line.DirInfo')
def test_dupes_no_cache(mock_DirInfo):
    """
    dupes command does populate if necessary.
    """
    mock_DirInfo.cached.return_value.file_count = 0
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.cached.assert_called_once_with(os.path.abspath('.'))
    mock_DirInfo.cached.return_value.populate.assert_called_once_with(
        **dupes_options()
    )
    mock_DirInfo.cached.return_value.dupe_groups.assert_called()

@patch('sys.argv', ['omnidiff', 'dupes', '.', '--populate'])
@patch('omnidiff.command_line.DirInfo')
def test_dupes_populate(mock_DirInfo):
    """
    dupes command populates if you tell it to.
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.cached.assert_called_once_with(os.path.abspath('.'))
    mock_DirInfo.cached.return_value.populate.assert_called_once_with(
        **dupes_options()
    )
    mock_DirInfo.cached.return_value.dupe_groups.assert_called()

@patch('sys.argv', ['omnidiff', 'dupes', '.', '--no-resume', '--populate'])
@patch('omnidiff.command_line.DirInfo')
def test_dupes_no_resume(mock_DirInfo):
    """
    Command-line supports populate() options: resume
    """
    with pytest.raises(SystemExit, match='0'):
        main()
    mock_DirInfo.return_value.populate.assert_called_once_with(
        **dupes_options(resume=False)
    )
    mock_DirInfo.return_value.dupe_groups.assert_called()
