import unittest
import sys
import paramiko
import socket
from unittest.mock import patch
from openfactory.ofacli import init_environment, main
from openfactory.kafka.ksql import KSQLDBClientException


class TestOFAEntryPoint(unittest.TestCase):
    """ Unit tests for OpenFactory ofacli.py entrypoint """

    @patch('openfactory.ofacli.ksql.connect')
    @patch('openfactory.ofacli.dal.connect')
    @patch('openfactory.ofacli.user_notify.setup')
    def test_init_environment_success(self, mock_notify, mock_dal_connect, mock_ksql_connect):
        """ init_environment returns True on successful setup """
        mock_ksql_connect.return_value = None

        result = init_environment()

        self.assertTrue(result)
        mock_notify.assert_called_once()
        mock_dal_connect.assert_called_once()
        mock_ksql_connect.assert_called_once()

    @patch('openfactory.ofacli.ksql.connect', side_effect=KSQLDBClientException("Connection failed"))
    @patch('openfactory.ofacli.dal.connect')
    @patch('openfactory.ofacli.user_notify')
    def test_init_environment_failure_ksql(self, mock_notify, mock_dal_connect, mock_ksql_connect):
        """ init_environment returns False if ksql.connect fails """
        result = init_environment()

        self.assertFalse(result)
        mock_notify.fail.assert_called_once_with('Failed to connect to ksqlDB server')

    @patch('openfactory.ofacli.ksql.connect')
    @patch(
        'openfactory.ofacli.dal.connect',
        side_effect=paramiko.ssh_exception.NoValidConnectionsError(
            errors={('127.0.0.1', 22): socket.error("Connection refused")}
        )
    )
    @patch('openfactory.ofacli.user_notify')
    def test_init_environment_dal_ssh_exception(self, mock_notify, mock_dal_connect, mock_ksql_connect):
        """ init_environment returns False if dal.connect fails due to SSH connection issues """
        result = init_environment()

        self.assertFalse(result)
        mock_notify.fail.assert_called_once()

    @patch('openfactory.ofacli.cli')
    @patch('openfactory.ofacli.init_environment', return_value=True)
    def test_main_runs_cli_if_env_init_ok(self, mock_init_env, mock_cli):
        """ main() calls cli() if init_environment() is successful """
        with patch.object(sys, 'argv', ['ofa', 'node']):  # A command that does NOT skip env setup
            main()
            mock_init_env.assert_called_once()
            mock_cli.assert_called_once()

    @patch('openfactory.ofacli.init_environment', return_value=False)
    @patch('openfactory.ofacli.exit', side_effect=SystemExit(1))
    def test_main_exits_on_failed_env_init(self, mock_exit, mock_init_env):
        """ main() exits if init_environment() fails """
        with patch.object(sys, 'argv', ['ofa', 'nodes', 'ls']):
            with self.assertRaises(SystemExit) as cm:
                main()

            mock_init_env.assert_called_once()
            mock_exit.assert_called_once_with(1)
            self.assertEqual(cm.exception.code, 1)

    @patch('openfactory.ofacli.ksql.connect')
    @patch('openfactory.ofacli.dal.connect')
    @patch('openfactory.ofacli.user_notify.setup')
    def test_init_environment_skips_ksql_when_disabled(self, mock_notify, mock_dal_connect, mock_ksql_connect):
        """ init_environment skips ksql connection when connect_ksql=False. """
        result = init_environment(connect_ksql=False)

        self.assertTrue(result)
        mock_dal_connect.assert_called_once()
        mock_ksql_connect.assert_not_called()

    @patch('openfactory.ofacli.cli')
    @patch('openfactory.ofacli.init_environment')
    def test_main_skippable_commands(self, mock_init_env, mock_cli):
        """
        main() calls init_environment(connect_ksql=False) for commands that
        skip full environment setup, and then calls cli().
        """
        skip_commands = [
            ['ofa', 'version'],
            ['ofa', '--help'],
            ['ofa', 'config'],
            ['ofa', 'templates'],
            ['ofa', 'nodes'],
        ]

        for argv in skip_commands:
            with self.subTest(argv=argv):
                with patch.object(sys, 'argv', argv):
                    main()

                mock_init_env.assert_called_once_with(connect_ksql=False)
                mock_cli.assert_called_once()

                mock_init_env.reset_mock()
                mock_cli.reset_mock()

    @patch('openfactory.ofacli.cli')
    @patch('openfactory.ofacli.init_environment')
    def test_main_normal_commands(self, mock_init_env, mock_cli):
        """
        main() calls init_environment() normally (connect_ksql=True) for commands
        that require full environment setup, and then calls cli().
        """
        normal_commands = [
            ['ofa', 'device'],
            ['ofa', 'some_other_command'],
        ]

        for argv in normal_commands:
            with self.subTest(argv=argv):
                with patch.object(sys, 'argv', argv):
                    main()

                mock_init_env.assert_called_once_with()  # default connect_ksql=True
                mock_cli.assert_called_once()

                mock_init_env.reset_mock()
                mock_cli.reset_mock()
