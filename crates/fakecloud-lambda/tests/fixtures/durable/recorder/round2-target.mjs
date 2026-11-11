export const handler = async (event) => {
  console.log("TARGET " + JSON.stringify(event));
  if (event.boom) throw new Error("target exploded");
  return { echo: event };
};
