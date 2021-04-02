module.exports = {
        apps: [
                {
                        name: "dashboard-07-21",
                        script: __dirname + '/../../main.py',
                        args: [ "-s", __dirname + '/settings.json' ],
                        interpreter: 'python3',
                        restart_delay: 60000,
                        cwd: __dirname,
                        max_memory_restart: "3G",
                }
        ]
};

