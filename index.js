const crypto = require('crypto')
const node_ssh = require('node-ssh')
const YAML = require('yaml')

module.export.warning = function (message, fileName, lineNumber) {
  const warning = Error(message, fileName, lineNumber)
  warning.logLevel = 'info'
  return warning
}
module.exports.info = function (message, fileName, lineNumber) {
  const info = Error(message, fileName, lineNumber)
  info.logLevel = 'info'
  return info
}

module.exports.datasetName = function (aString) {
  /* 
   * trim the name, we remove ddf-[-], remove gapminder-[-], 
   * remove big-waffle-, replace open_numbers-[-] with on_
   */
  let name = aString.toLowerCase()
  name = name.replace(/ddf-+/, '')
  name = name.replace(/gapminder-+/, '')
  name = name.replace(/big-*waffle-+/, '')
  name = name.replace(/open_numbers-+/, 'on_')
  return name  
}

let config
module.exports.getConfig = function () {
  return new Promise((resolve, reject) => {
    if (config) {
      resolve(config)
    } else {
      const GCS = require('@google-cloud/storage').Storage
      const { WritableStream } = require('memory-streams')
      
      const Bucket = (new GCS()).bucket(process.env['CONFIG_BUCKET'] || 'org-gapminder-big-waffle-functions')
      const keyFile = Bucket.file(process.env['CONFIG_FILE'] || `${process.env.FUNCTION_NAME.toLowerCase()}.yaml`)
      const buffer = new WritableStream()
      
      try {
        keyFile.createReadStream()
        .on('error', err => {
          console.error(err)
          reject(err)
        })
        .on('end', () => {
          config = YAML.parse(buffer.toString())
          resolve(config)
        })
        .pipe(buffer)
      } catch (err) {
        console.error(err)
        reject(err)
      }
    }
  })
}
  
module.exports.exec = function (cmd, config, res, content) {
  // ssh into the big-waffle master and execute the command
  const ssh = new node_ssh()
  return ssh.connect({
    host: config.bwMaster,
    username: config.user || process.env.FUNCTION_NAME.toLowerCase(),
    privateKey: config.privateKey
  })
  .then(shell => {
    shell.exec(cmd)
  })
  .then(() => {
    if (res) res.send(content || 'OK')     
  })
  .catch(err => {
    // TODO: use bunyan for Stackdriver to do the logging
    if ((err.logLevel || 'error') == 'error') {
      console.error(err)
    } else {
      console.log(err.message)
    }
    if (res) res.send(content || 'OK')     
  })
}