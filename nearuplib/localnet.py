import json
import logging
import mergedeep
import os
import pathlib
import shutil
import sys

from nearuplib.constants import NODE_PID_FILE, LOCALNET_FOLDER, LOCALNET_LOGS_FOLDER
from nearuplib.nodelib import run_binary, proc_name_from_pid, is_neard_running
from nearuplib import util

_LOCALNET_RPC_PORT = 3030
_LOCALNET_NETWORK_PORT = 24567


def run(binary_path,
        home,
        num_validators,
        num_non_validators,
        num_shards,
        override,
        fix_accounts,
        archival_nodes,
        rpc_nodes,
        tracked_shards,
        verbose=True,
        interactive=False,
        config_override_path=None,
        genesis_override_path=None,
        log_level=None,
        opentelemetry=None):
    home = pathlib.Path(home)

    def read_json_from_file(path):
        assert path.endswith('.json')
        with open(path, 'r') as file:
            return json.loads(file.read())

    def read_json_for_node(file_name, node_id):
        assert file_name.endswith('.json')
        path = home / f'node{node_id}' / file_name
        assert path.exists()
        return json.loads(path.read_text())

    def write_json_for_node(file_name, node_id, data):
        assert file_name.endswith('.json')
        path = home / f'node{node_id}' / file_name
        path.write_text(json.dumps(data, indent=2))

    if home.exists():
        if util.prompt_bool_flag(
                'Would you like to remove data from the previous localnet run?',
                override,
                interactive=interactive):
            print("Removing old data since 'override' flag is set.")
            shutil.rmtree(home)
    elif interactive:
        print(
            util.wraptext('''
            Starting localnet NEAR nodes.  This is a test network entirely local
            to this machine.  Validators and non-validating nodes will be
            started, and will communicate with each other on localhost,
            producing blocks on top of a genesis block generated locally.
        '''))
        print()

    if not home.exists():
        num_validators = util.prompt_flag(
            'How many validator nodes would you like to initialize this localnet with?',
            num_validators,
            default=4,
            interactive=interactive,
            type=int,
        )
        num_non_validators = util.prompt_flag(
            'How many non-validator nodes would you like to initialize this localnet with?',
            num_non_validators,
            default=0,
            interactive=interactive,
            type=int,
        )
        num_shards = util.prompt_flag(
            'How many shards would you like to initialize this localnet with?'
            '\nSee https://near.org/papers/nightshade/#sharding-basics',
            num_shards,
            default=1,
            interactive=interactive,
            type=int,
        )
        fixed_shards = False
        if num_shards > 1:
            fixed_shards = util.prompt_bool_flag(
                'Would you like to setup fixed accounts for first (N-1) shards (shard0, shard1, ...)?',
                fix_accounts,
                interactive=interactive,
            )
        archival_nodes = util.prompt_flag(
            "What nodes should be archival nodes (keep full history)?",
            archival_nodes,
            interactive=interactive,
            default="")
        rpc_nodes = util.prompt_flag(
            "What nodes should be archival nodes (keep full history)?",
            rpc_nodes,
            interactive=interactive,
            default="")
        tracked_shards = util.prompt_flag(
            "What shards should be tracked? Comma separated list of shards to track, the word \'all\' to track all shards or the word \'none\' to track no shards.",
            tracked_shards,
            interactive=interactive,
            default="all")

        run_binary(
            binary_path,
            home,
            'localnet',
            shards=num_shards,
            validators=num_validators,
            non_validators=num_non_validators,
            fixed_shards=fixed_shards,
            archival_nodes=archival_nodes,
            rpc_nodes=rpc_nodes,
            tracked_shards=tracked_shards,
            opentelemetry=opentelemetry,
            print_command=interactive,
        ).wait()

    num_nodes = num_validators + num_non_validators

    # Edit configuration files for specific nodes.
    for node_id in range(0, num_nodes):
        # Update the default config with overrides and write it back.
        config = read_json_for_node('config.json', node_id)
        if config_override_path:
            config_override = read_json_from_file(config_override_path)
            mergedeep.merge(config,
                            config_override,
                            strategy=mergedeep.Strategy.TYPESAFE_REPLACE)
        # Override the ports based on the node id.
        config['rpc']['addr'] = f'0.0.0.0:{_LOCALNET_RPC_PORT + node_id}'
        config['network'][
            'addr'] = f'0.0.0.0:{_LOCALNET_NETWORK_PORT + node_id}'
        write_json_for_node('config.json', node_id, config)

        # Update the default genesis config with overrides and write it back.
        genesis = read_json_for_node('genesis.json', node_id)
        if genesis_override_path:
            genesis_override = read_json_from_file(genesis_override_path)
            mergedeep.merge(genesis,
                            genesis_override,
                            strategy=mergedeep.Strategy.TYPESAFE_REPLACE)
        write_json_for_node('genesis.json', node_id, genesis)

        # Write log config.
        log_config = {
            'opentelemetry':
                str(opentelemetry).lower()
                if opentelemetry is not None else None,
            'rust_log':
                str(log_level).upper() if log_level is not None else None
        }
        write_json_for_node('log_config.json', node_id, log_config)

    # Load public key from first node
    node_key = read_json_for_node('node_key.json', 0)
    public_key = node_key['public_key']

    # Recreate log folder
    shutil.rmtree(LOCALNET_LOGS_FOLDER, ignore_errors=True)
    os.mkdir(LOCALNET_LOGS_FOLDER)

    # Spawn network
    with open(NODE_PID_FILE, 'w') as pid_fd:
        for i in range(0, num_nodes):
            proc = run_binary(
                binary_path,
                os.path.join(home, f'node{i}'),
                'run',
                verbose=verbose,
                boot_nodes=f'{public_key}@127.0.0.1:24567' if i > 0 else None,
                output=os.path.join(LOCALNET_LOGS_FOLDER, f'node{i}'),
                print_command=interactive)
            proc_name = proc_name_from_pid(proc.pid)
            pid_fd.write(f'{proc.pid}|{proc_name}|localnet\n')

    logging.info('Localnet was spawned successfully...')
    logging.info(f'Localnet logs written in: {LOCALNET_LOGS_FOLDER}')
    logging.info('Check localnet status at http://127.0.0.1:3030/status')


def entry(binary_path, home, num_validators, num_non_validators, num_shards,
          override, fix_accounts, archival_nodes, rpc_nodes, tracked_shards,
          verbose, interactive, config_override_path, genesis_override_path,
          log_level, opentelemetry):
    if binary_path:
        binary_path = os.path.join(binary_path, 'neard')
    else:
        uname = os.uname()[0]
        binary_path = os.path.join(LOCALNET_FOLDER, 'neard')
        if not os.path.exists(LOCALNET_FOLDER):
            os.makedirs(LOCALNET_FOLDER)
        util.download_binaries('localnet', uname)

    if is_neard_running():
        sys.exit(1)

    run(binary_path, home, num_validators, num_non_validators, num_shards,
        override, fix_accounts, archival_nodes, rpc_nodes, tracked_shards,
        verbose, interactive, config_override_path, genesis_override_path,
        log_level, opentelemetry)
