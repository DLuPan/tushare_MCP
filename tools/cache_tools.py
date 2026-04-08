"""缓存管理MCP工具"""
from typing import TYPE_CHECKING
from cache.cache_manager import cache_manager

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

def register_cache_tools(mcp: "FastMCP"):
 
    @mcp.tool()
    def get_cache_stats() -> str:
        """获取缓存统计信息"""
        try:
            stats = cache_manager.get_stats()
            result = []
            result.append("📊 缓存统计信息")
            result.append("=" * 40)
            
            total = stats.pop('_total', {})
            result.append(f"\n总计：{total.get('count', 0)} 条缓存，总访问次数：{total.get('total_access', 0)}")
            result.append("\n按类型统计：")
            
            for cache_type, data in stats.items():
                result.append(f"  • {cache_type}: {data.get('count', 0)} 条，访问 {data.get('total_access', 0)} 次")
            
            return "\n".join(result)
        except Exception as e:
            return f"获取缓存统计失败：{str(e)}"
