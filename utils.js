exports.parseFloatWith = (regex, input) => {
  const res = regex.exec(input);
  return parseFloat(res[1]);
};

exports.calcSum = (arr, key) => {
  return parseFloat(
    arr
      .reduce((prev, curr) => {
        return prev + curr[key];
      }, 0)
      .toFixed(2)
  );
};
