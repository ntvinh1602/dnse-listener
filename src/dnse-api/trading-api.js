import { requestDnse } from "./client.js"

/**
 * @param {string} accountNo
 * @param {number} orderId
 */
export async function getOrderDetails(accountNo, orderId) {
  return requestDnse(
    "GET",
    `/accounts/${encodeURIComponent(accountNo)}/orders/${encodeURIComponent(orderId)}`,
    {
      query: {
        marketType: "STOCK",
        orderCategory: "NORMAL",
      },
    },
  )
}
