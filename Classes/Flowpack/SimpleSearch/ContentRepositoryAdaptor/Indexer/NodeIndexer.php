<?php
namespace Flowpack\SimpleSearch\ContentRepositoryAdaptor\Indexer;

use TYPO3\Flow\Annotations as Flow;
use TYPO3\TYPO3CR\Domain\Model\NodeInterface;
use TYPO3\TYPO3CR\Domain\Service\NodeTypeManager;


/**
 * Indexer for Content Repository Nodes.
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexer extends \TYPO3\TYPO3CR\Search\Indexer\AbstractNodeIndexer {

	/**
	 * @Flow\Inject
	 * @var \Flowpack\SimpleSearch\Domain\Service\IndexInterface
	 */
	protected $indexClient;

	/**
	 * @Flow\Inject
	 * @var \TYPO3\Flow\Persistence\PersistenceManagerInterface
	 */
	protected $persistenceManager;

	/**
	 * @Flow\Inject
	 * @var NodeTypeManager
	 */
	protected $nodeTypeManager;

	/**
	 * @Flow\Inject
	 * @var \TYPO3\TYPO3CR\Domain\Service\ContentDimensionPresetSourceInterface
	 */
	protected $contentDimensionPresetSource;

	/**
	 * @Flow\Inject
	 * @var \TYPO3\TYPO3CR\Domain\Repository\WorkspaceRepository
	 */
	protected $workspaceRepository;

	/**
	 * @Flow\Inject
	 * @var \TYPO3\TYPO3CR\Domain\Service\ContextFactoryInterface
	 */
	protected $contextFactory;

	/**
	 * the default context variables available inside Eel
	 *
	 * @var array
	 */
	protected $defaultContextVariables;

	/**
	 * @var array
	 */
	protected $fulltextRootNodeTypes = array();

	protected $indexedNodeData = array();

	/**
	 * Called by the Flow object framework after creating the object and resolving all dependencies.
	 *
	 * @param integer $cause Creation cause
	 */
	public function initializeObject($cause) {
		parent::initializeObject($cause);
		foreach ($this->nodeTypeManager->getNodeTypes() as $nodeType) {
			$searchSettingsForNodeType = $nodeType->getConfiguration('search');
			if (is_array($searchSettingsForNodeType) && isset($searchSettingsForNodeType['fulltext']['isRoot']) && $searchSettingsForNodeType['fulltext']['isRoot'] === TRUE) {
				$this->fulltextRootNodeTypes[] = $nodeType->getName();
			}
		}
	}

	/**
	 * @return \Flowpack\SimpleSearch\Domain\Service\IndexInterface
	 */
	public function getIndexClient() {
		return $this->indexClient;
	}

	/**
	 * index this node, and add it to the current bulk request.
	 *
	 * @param NodeInterface $node
	 * @param string $targetWorkspaceName
	 * @return void
	 */
	public function indexNode(NodeInterface $node, $targetWorkspaceName = NULL) {
		$identifier = $this->generateUniqueNodeIdentifier($node);
		if ($node->isRemoved()) {
			$this->indexClient->removeData($identifier);
			return;
		}

		$workspaceKey = '#'.$node->getWorkspace()->getName().'#';
		$this->removeWorkspaceFromExistingEntries($node, $workspaceKey);
		$this->updateNodeIndex($node, $workspaceKey);
	}

	/**
	 * Remove workspace from all node entries that reference it.
	 * Removes orphans without any workspace they relate to.
	 *
	 * @param NodeInterface $node
	 * @param string        $workspaceKey
	 */
	protected function removeWorkspaceFromExistingEntries(NodeInterface $node, $workspaceKey) {
		$allIndexedVariants = $this->indexClient->executeStatement('SELECT * FROM objects WHERE __identifier = :nodeIdentifier', array(
			':nodeIdentifier' => $node->getIdentifier()
		));
		foreach ($allIndexedVariants as $nodeVariant) {
			if (strpos($nodeVariant['__workspace'], $workspaceKey) !== false) {
				$removeCurrentWs = str_replace(
					$workspaceKey,
					'',
					$nodeVariant['__workspace']
				);
				// if entry has no related workspaces left delete, update otherwiese
				if (!$removeCurrentWs) {
					$this->indexClient->removeData($nodeVariant['__identifier__']);
				} else {
					$nodeVariant['__workspace'] = $removeCurrentWs;
					$this->indexClient->insertOrUpdatePropertiesToIndex($nodeVariant, $nodeVariant['__identifier__']);
				}
			}
		}
	}

	/**
	 * Update the properties and fulltext for the given node.
	 * Also adds workspaces from old entry if one exists.
	 *
	 * @param NodeInterface $node
	 * @param string        $workspaceKey
	 */
	protected function updateNodeIndex(NodeInterface $node, $workspaceKey) {
		$fulltextData = array();
		$nodePropertiesToBeStoredInIndex = $this->extractPropertiesAndFulltext($node, $fulltextData);
		if (count($fulltextData) !== 0) {
			$this->addFulltextToRoot($node, $fulltextData);
		}

		$identifier = $this->generateUniqueNodeIdentifier($node);
		$oldEntry = $this->indexClient->findOneByIdentifier($identifier);
		if ($oldEntry && array_key_exists('__workspace', $oldEntry)) {
			// keep complete list of all workspaces this entry exists in
			$nodePropertiesToBeStoredInIndex['__workspace'] = $oldEntry['__workspace'];

			// add current workspace if reintegrating into existing entry
			if (strpos($nodePropertiesToBeStoredInIndex['__workspace'], $workspaceKey) === false) {
				$nodePropertiesToBeStoredInIndex['__workspace'] .= ', '.$workspaceKey;
			}
		}

		$this->indexClient->indexData($identifier, $nodePropertiesToBeStoredInIndex, $fulltextData);
	}

	/**
	 * @param NodeInterface $node
	 * @return void
	 */
	public function removeNode(NodeInterface $node) {
		$identifier = $this->generateUniqueNodeIdentifier($node);
		$this->indexClient->removeData($identifier);
	}

	/**
	 * @return void
	 */
	public function flush() {
		$this->indexedNodeData = array();
	}

	/**
	 * @param string $workspaceName
	 */
	protected function indexNodeInWorkspace($nodeIdentifier, $workspaceName) {
		$dimensionCombinations = $this->calculateDimensionCombinations();
		if ($dimensionCombinations !== array()) {
			foreach ($dimensionCombinations as $combination) {
				$context = $this->contextFactory->create(array('workspaceName' => $workspaceName, 'dimensions' => $combination));
				$node = $context->getNodeByIdentifier($nodeIdentifier);
				if ($node !== NULL) {
					$this->indexNode($node, NULL, FALSE);
				}
			}
		} else {
			$context = $this->contextFactory->create(array('workspaceName' => $workspaceName));
			$node = $context->getNodeByIdentifier($nodeIdentifier);
			if ($node !== NULL) {
				$this->indexNode($node, NULL, FALSE);
			}
		}
	}

	/**
	 * @param NodeInterface $node
	 * @param array $fulltext
	 */
	protected function addFulltextToRoot(NodeInterface $node, $fulltext) {
		$fulltextRoot = $this->findFulltextRoot($node);
		if ($fulltextRoot !== NULL) {
			$identifier = $this->generateUniqueNodeIdentifier($fulltextRoot);
			$this->indexClient->addToFulltext($fulltext, $identifier);
		}
	}

	/**
	 * @param NodeInterface $node
	 * @return NodeInterface
	 */
	protected function findFulltextRoot(NodeInterface $node) {
		if (in_array($node->getNodeType()->getName(), $this->fulltextRootNodeTypes)) {
			return NULL;
		}

		$currentNode = $node->getParent();
		while ($currentNode !== NULL) {
			if (in_array($currentNode->getNodeType()->getName(), $this->fulltextRootNodeTypes)) {
				return $currentNode;
			}

			$currentNode = $currentNode->getParent();
		}

		return NULL;
	}

	/**
	 * Generate identifier for index entry based on node identifier and context
	 *
	 * @param NodeInterface $node
	 * @return string
	 */
	protected function generateUniqueNodeIdentifier(NodeInterface $node) {
		$nodeDataPersistenceIdentifier = $this->persistenceManager->getIdentifierByObject($node->getNodeData());
		return $nodeDataPersistenceIdentifier;
	}

	/**
	 * @return array
	 */
	public function calculateDimensionCombinations() {
		$dimensionPresets = $this->contentDimensionPresetSource->getAllPresets();

		$dimensionValueCountByDimension = array();
		$possibleCombinationCount = 1;
		$combinations = array();

		foreach ($dimensionPresets as $dimensionName => $dimensionPreset) {
			if (isset($dimensionPreset['presets']) && !empty($dimensionPreset['presets'])) {
				$dimensionValueCountByDimension[$dimensionName] = count($dimensionPreset['presets']);
				$possibleCombinationCount = $possibleCombinationCount * $dimensionValueCountByDimension[$dimensionName];
			}
		}

		foreach ($dimensionPresets as $dimensionName => $dimensionPreset) {
			for ($i = 0; $i < $possibleCombinationCount; $i++) {
				if (!isset($combinations[$i]) || !is_array($combinations[$i])) {
					$combinations[$i] = array();
				}

				$currentDimensionCurrentPreset = current($dimensionPresets[$dimensionName]['presets']);
				$combinations[$i][$dimensionName] = $currentDimensionCurrentPreset['values'];

				if (!next($dimensionPresets[$dimensionName]['presets'])) {
					reset($dimensionPresets[$dimensionName]['presets']);
				}
			}
		}

		return $combinations;
	}

}
