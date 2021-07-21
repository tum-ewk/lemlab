const Migrations = artifacts.require("migrations");

module.exports = function(deployer) {
  deployer.deploy(Migrations);
};
