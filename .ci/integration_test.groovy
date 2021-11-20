def call(ghprbActualCommit, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId) {

    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"

    // parse pd branch
    def m2 = ghprbCommentBody =~ /pd\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m2) {
        PD_BRANCH = "${m2[0][1]}"
    }
    m2 = null
    println "PD_BRANCH=${PD_BRANCH}"

    // parse tikv branch
    def m3 = ghprbCommentBody =~ /tikv\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m3) {
        TIKV_BRANCH = "${m3[0][1]}"
    }
    m3 = null
    println "TIKV_BRANCH=${TIKV_BRANCH}"

    catchError {
        node ('build') {
            container("java") {
                stage('Prepare') {
                    dir("/home/jenkins/agent/git/client-java") {
                        sh """
                        rm -rf /maven/.m2/repository/*
                        rm -rf /maven/.m2/settings.xml
                        rm -rf ~/.m2/settings.xml
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/client-java/cache/tikv-client-java-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                        """
                        if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                            deleteDir()
                        }
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: credentialsId, refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:tikv/client-java.git']]]
                        sh "git checkout -f ${ghprbActualCommit}"
                    }

                    dir("/home/jenkins/agent/git/client-java/_run") {
                        // tikv
                        def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"
                        // pd
                        def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"

                        sh """
                        killall -9 tikv-server || true
                        killall -9 pd-server || true
                        killall -9 java || true
                        sleep 10
                        """

                        sh """
                        echo "start TiKV for RawKV test"
                        bin/pd-server --name=pd_rawkv --data-dir=pd_rawkv --client-urls="http://0.0.0.0:2379" --advertise-client-urls="http://127.0.0.1:2379" --peer-urls="http://0.0.0.0:2380" --advertise-peer-urls="http://127.0.0.1:2380" --config=../config/pd.toml &>pd_rawkv.log &
                        sleep 10
                        bin/tikv-server --pd 127.0.0.1:2379 --data-dir tikv_rawkv --addr 0.0.0.0:20160 --advertise-addr 127.0.0.1:20160 --status-addr 0.0.0.0:20180 --config ../config/tikv_rawkv.toml &>tikv_rawkv.log &
                        sleep 10
                        ps aux | grep '-server' || true
                        curl -s 127.0.0.1:2379/pd/api/v1/status || true
                        """

                        sh """
                        echo "start TiKV for TxnKV test"
                        bin/pd-server --name=pd_txnkv --data-dir=pd_txnkv --client-urls="http://0.0.0.0:3379" --advertise-client-urls="http://127.0.0.1:3379" --peer-urls="http://0.0.0.0:3380" --advertise-peer-urls="http://127.0.0.1:3380" --config=../config/pd.toml &>pd_txnkv.log &
                        sleep 10
                        bin/tikv-server --pd 127.0.0.1:3379 --data-dir tikv_txnkv --addr 0.0.0.0:21160 --advertise-addr 127.0.0.1:21160 --status-addr 0.0.0.0:21180 --config ../config/tikv_txnkv.toml &>tikv_txnkv.log &
                        sleep 10
                        ps aux | grep '-server' || true
                        curl -s 127.0.0.1:3379/pd/api/v1/status || true
                        """

                        sh "sleep 30"
                    }
                }

                stage('Test') {
                    dir("/home/jenkins/agent/git/client-java") {
                        try {
                            timeout(30) {
                                sh ".ci/test.sh"
                            }
                        } catch (err) {
                            sh """
                            ps aux | grep '-server' || true
                            curl -s 127.0.0.1:2379/pd/api/v1/status || true
                            """
                            sh "cat _run/pd_rawkv.log"
                            sh "cat _run/tikv_rawkv.log"
                            sh "cat _run/pd_txnkv.log"
                            sh "cat _run/tikv_txnkv.log"
                            throw err
                        }
                    }
                }
            }
        }
        currentBuild.result = "SUCCESS"
    }

    stage('Summary') {
        def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
        def msg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
        "${ghprbPullLink}" + "\n" +
        "${ghprbPullDescription}" + "\n" +
        "Integration Common Test Result: `${currentBuild.result}`" + "\n" +
        "Elapsed Time: `${duration} mins` " + "\n" +
        "${env.RUN_DISPLAY_URL}"

        print msg
    }
}

return this
